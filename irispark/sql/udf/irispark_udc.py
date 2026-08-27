"""irispark_udc -- pure-Python UDF semantics for IRISpark.

Reimplements five PySpark 3.5.8 SQL functions (``conv``,
``format_string``/``printf``, ``parse_url``, ``from_utc_timestamp``,
``to_utc_timestamp``) so their exact observable behavior can run inside
IRIS's embedded Python.

Every rule below was derived from live pins captured on PySpark 3.5.8
(22 conv base pairs x 32 values, 55 format_string cases, 168 parse_url
cells, 34 timezone cells).  Cells outside the pinned domain follow from the
same code path but are documented in ``non_covered.md`` as best-effort.

Boundary conventions (SQL <-> ObjectScript <-> Python):

* SQL NULL arrives as the empty string and is returned as ``None``, which
  ObjectScript returns as SQL NULL.  This follows the existing
  ``irispark_months_between`` convention.
* A genuine empty string in input is therefore indistinguishable from SQL
  NULL at the boundary.  Affected edges are documented as deviations.

The module is importable and testable without any IRIS dependency; the
ObjectScript thunks in ``epython.py`` invoke its public functions via
``##class(%SYS.Python).Import("irispark_udc")``.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

_MASK64 = (1 << 64) - 1
_INT32_MIN, _INT32_MAX = -(1 << 31), (1 << 31) - 1
_DIGS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _digit_value(c: str) -> int:
    if "0" <= c <= "9":
        return ord(c) - 48
    if "a" <= c <= "z":
        return ord(c) - 87
    if "A" <= c <= "Z":
        return ord(c) - 55
    return -1


def _signed64(v: int) -> int:
    """Reinterpret an accumulated value as a signed 64-bit long, matching
    Java's silent two's-complement wrap during Spark's accumulation."""
    v &= _MASK64
    return v - (1 << 64) if v >= (1 << 63) else v


def _digits(num: int, base: int) -> str:
    if num == 0:
        return "0"
    out = []
    n = num
    while n:
        n, r = divmod(n, base)
        out.append(_DIGS[r])
    return "".join(reversed(out))


def _round_half_up(value: Decimal, places: int) -> Decimal:
    q = Decimal(1).scaleb(-places)
    return value.quantize(q, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# conv
# ---------------------------------------------------------------------------

def conv(num, from_base, to_base):
    """PySpark ``conv`` semantics (3.5.8 pin matrix).

    Parsing: trim; optional leading ``-`` (a ``+`` is NOT accepted);
    consume digits valid for ``from_base``, stopping at the first invalid
    character.  Values accumulate in a signed 64-bit long with silent
    two's-complement wrap (``9223372036854775808`` base 10 ->
    ``1000...0`` base 2).

    Output: for ``to_base > 0`` the unsigned 64-bit reinterpretation is
    printed (upper-case letters, no padding); for ``to_base < 0`` the
    signed magnitude is printed (``-`` prefix when negative, including
    the ``-0`` quirk when a lone ``-`` was consumed and no digit
    followed).  ``from_base <= 0`` yields NULL for every input.

    Pinned deviation: ``to_base == -2`` crashes the Spark stage (JVM
    ArrayIndexOutOfBounds); here it raises, which surfaces as a SQL
    error -- the same failure class, documented in non_covered.md.
    ``to_base`` 0/1 and ``from_base`` > 36 are unpinned (see
    non_covered.md); they return NULL.
    """
    if num is None or from_base is None or to_base is None:
        return None
    fb = int(from_base)
    tb = int(to_base)
    if fb <= 0 or tb == 0 or tb == 1:
        return None
    s = str(num).strip()
    if s == "":
        return None
    neg = False
    if s.startswith("-"):
        neg = True
        s = s[1:]
    value = 0
    consumed = 0
    for c in s:
        d = _digit_value(c)
        if d < 0 or d >= fb:
            break
        value = (value * fb + d) & _MASK64
        consumed += 1
    signed = -_signed64(value) if neg else _signed64(value)
    if tb > 0:
        return _digits(signed & _MASK64, tb)
    mag = abs(signed)
    out = _digits(mag, -tb)
    if signed < 0 or (neg and consumed == 0):
        out = "-" + out
    return out


# ---------------------------------------------------------------------------
# format_string / printf
# ---------------------------------------------------------------------------

class _FormatError(ValueError):
    pass


def _group_thousands(s: str) -> str:
    out = []
    for i, c in enumerate(reversed(s)):
        if i and i % 3 == 0:
            out.append(",")
        out.append(c)
    return "".join(reversed(out))


def _num_bits(v: int) -> int:
    """Spark literal typing: values that fit int32 format as 32-bit two's
    complement in ``%x``/``%o``, larger ones as 64-bit."""
    return 32 if _INT32_MIN <= v <= _INT32_MAX else 64


def _pad(body: str, width: int, flags: str, left_ok: bool = True) -> str:
    if width <= len(body):
        return body
    pad_w = width - len(body)
    if "-" in flags:
        return body + " " * pad_w
    if "0" in flags and left_ok:
        prefix = ""
        rest = body
        if body and body[0] in "-+ ":
            prefix, rest = body[0], body[1:]
        return prefix + "0" * pad_w + rest
    return " " * pad_w + body


def _sign(v, flags: str) -> str:
    if v < 0 or (v == 0 and repr(v).startswith("-")):
        return "-"
    if "+" in flags:
        return "+"
    if " " in flags:
        return " "
    return ""


def _format_d(v: int, flags: str, width: int,
              precision: int | None) -> str:
    sign = _sign(v, flags)
    mag = str(abs(v)).zfill(precision or 0)
    if "," in flags:
        mag = _group_thousands(mag)
    return _pad(sign + mag, width, flags)


def _format_x(v: int, flags: str, width: int,
              precision: int | None, upper: bool) -> str:
    u = v & ((1 << _num_bits(v)) - 1)
    mag = _digits(u, 16)
    if not upper:
        mag = mag.lower()
    mag = mag.zfill(precision or 0)
    if "#" in flags and v != 0:
        mag = ("0X" if upper else "0x") + mag
    return _pad(mag, width, flags, left_ok=False)


def _format_o(v: int, flags: str, width: int,
              precision: int | None) -> str:
    u = v & ((1 << _num_bits(v)) - 1)
    mag = _digits(u, 8)
    mag = mag.zfill(precision or 0)
    if "#" in flags and v != 0:
        mag = "0" + mag
    return _pad(mag, width, flags, left_ok=False)


def _format_f(v: float, flags: str, width: int, precision: int) -> str:
    sign = _sign(v, flags)
    d = _round_half_up(Decimal(repr(abs(v))), precision)
    mag = format(d, "f")
    if "#" in flags and "." not in mag:
        mag += "."
    return _pad(sign + mag, width, flags)


def _format_e(v: float, flags: str, width: int, precision: int,
              upper: bool) -> str:
    sign = _sign(v, flags)
    d = Decimal(repr(abs(v)))
    if d == 0:
        exp = 0
        mant_r = Decimal("0")
    else:
        exp = d.adjusted()
        mant_r = _round_half_up(d.scaleb(-exp), precision)
        if mant_r >= 10:
            mant_r = mant_r.scaleb(-1)
            exp += 1
    ms = format(mant_r, "f")
    prefix, _, frac = ms.partition(".")
    if precision == 0:
        ms = prefix + ("." if "#" in flags else "")
    else:
        ms = prefix + "." + frac.ljust(precision, "0")
    echar = "E" if upper else "e"
    esign = "+" if exp >= 0 else "-"
    return _pad(sign + ms + echar + esign + str(abs(exp)).zfill(2),
                width, flags)


def _format_g(v: float, flags: str, width: int, precision: int,
              upper: bool) -> str:
    """Java %g: precision significant digits, 'e' form when the
    rounded exponent is < -4 or >= precision, else 'f' with trailing
    zeros stripped."""
    if precision <= 0:
        precision = 1
    d = Decimal(repr(abs(v)))
    if d == 0:
        return _pad("0", width, flags)
    exp = d.adjusted()
    digits = _round_half_up(d.scaleb(-exp), precision - 1)
    if digits >= 10:
        digits = digits.scaleb(-1)
        exp += 1
    use_e = exp < -4 or exp >= precision
    if use_e:
        ms = format(digits, "f")
        if "#" not in flags:
            ms = ms.rstrip("0").rstrip(".")
        echar = "E" if upper else "e"
        esign = "+" if exp >= 0 else "-"
        zpad = "0" * max(0, 2 - len(str(abs(exp))))
        body = ms + echar + esign + zpad + str(abs(exp))
    else:
        body = format(_round_half_up(d, precision - 1 - exp), "f")
        if "#" not in flags:
            body = body.rstrip("0").rstrip(".")
    return _pad(_sign(v, flags) + body, width, flags)


def _format_s(value, flags: str, width: int,
              precision: int | None) -> str:
    if value is None:
        s = "null"
    elif isinstance(value, str):
        s = value
    elif isinstance(value, bool):
        s = "true" if value else "false"
    else:
        s = str(value)
    if precision is not None:
        s = s[:precision]
    return _pad(s, width, flags)


def _format_c(value, flags: str, width: int) -> str:
    if value is None:
        s = "null"
    else:
        s = chr(int(value))
    return _pad(s, width, flags)


def _format_b(value) -> str:
    if value is None:
        return "false"
    if isinstance(value, str):
        return "false" if value == "" else "true"
    return "true" if bool(value) else "false"


_SPEC_RE = re.compile(
    r"^%(?P<idx>\d+\$)?(?P<flags>[-#+ 0,]*)(?P<width>\d+)?"
    r"(?P<prec>\.\d+)?(?P<conv>[a-zA-Z%])"
)


def _try_format_spec(spec: str, args: list, pos: int):
    m = _SPEC_RE.match(spec)
    if not m:
        raise _FormatError("Invalid format specification: " + spec)
    conv = m.group("conv")
    if conv == "%":
        return "%", pos
    flags = m.group("flags") or ""
    width = int(m.group("width")) if m.group("width") else 0
    prec = m.group("prec")
    precision = int(prec[1:]) if prec else None
    if m.group("idx"):
        idx = int(m.group("idx")[:-1]) - 1
        if idx < 0 or idx >= len(args):
            raise _FormatError("MissingFormatArgumentException")
    else:
        idx = pos
    if idx >= len(args):
        raise _FormatError("MissingFormatArgumentException")
    value = args[idx]
    if conv == "i":
        raise _FormatError("Conversion = 'i'")
    if conv in "dioxX":
        if value is None:
            return "null", pos + 1
        if isinstance(value, bool):
            raise _FormatError("IllegalFormatConversionException")
        if not isinstance(value, int):
            raise _FormatError("IllegalFormatConversionException")
        if conv == "d":
            r = _format_d(value, flags, width, precision)
        elif conv == "o":
            r = _format_o(value, flags, width, precision)
        else:
            r = _format_x(value, flags, width, precision, conv == "X")
    elif conv in "feEgG":
        if value is None:
            return "null", pos + 1
        v = float(value)
        p = precision if precision is not None else 6
        if conv in "feE":
            r = _format_f(v, flags, width, p) if conv == "f" else \
                _format_e(v, flags, width, p, conv == "E")
        else:
            r = _format_g(v, flags, width, p, conv == "G")
    elif conv == "s":
        r = _format_s(value, flags, width, precision)
    elif conv == "c":
        r = _format_c(value, flags, width)
    elif conv == "b":
        r = _format_b(value)
    elif conv == "n":
        r = "\n"
    else:
        raise _FormatError("Conversion = '" + conv + "'")
    return r, pos + 1


def format_string(fmt, *args):
    """PySpark ``format_string`` (Java ``String.format``) semantics.

    Mirrors the pinned 3.5.8 cell set: ``%s/d/o/x/X/f/e/E/g/G/c/b`` plus
    ``%%``.  ``%i`` raises (Conversion = 'i'); ``%d``/``%f`` with NULL
    produce the literal ``null``; ``%d`` with a non-integer raises;
    ``%x``/``%o`` print 32-bit two's complement for values that fit in
    int32 and 64-bit otherwise; ``%e`` exponents are zero-padded to two
    digits with an explicit sign; rounding is half-up; positional
    arguments (``%2$s``) are supported.

    Deviation: the SQL surface pads calls to a fixed 8-argument
    signature, so a format referencing more arguments than were provided
    prints ``null`` (Java raises MissingFormatArgumentException).
    Formats needing more than 8 arguments raise at plan time in the
    wrapper (irispark/functions.py) -- documented in non_covered.md.
    """
    if fmt is None:
        return None
    out = []
    i = 0
    pos = 0
    n = len(fmt)
    while i < n:
        c = fmt[i]
        if c != "%":
            out.append(c)
            i += 1
            continue
        j = i
        while j < n and fmt[j] == "%":
            j += 1
        cnt = j - i
        out.append("%" * (cnt // 2))
        if cnt % 2 == 1:
            if j == n:
                raise _FormatError("Incomplete format spec")
            while j < n and not fmt[j].isalpha():
                j += 1
            if j == n:
                raise _FormatError("Incomplete format specification")
            spec = fmt[i + cnt - 1:j + 1]
            r, pos = _try_format_spec(spec, list(args), pos)
            out.append(r)
            i = j + 1
        else:
            i = j
    return "".join(out)


def printf(fmt, *args):
    """Alias of :func:`format_string` (PySpark ``printf`` delegates to
    the same builder)."""
    return format_string(fmt, *args)


# ---------------------------------------------------------------------------
# parse_url
# ---------------------------------------------------------------------------

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*):")
_ABS_PATH_AT = re.compile(r"^(//)([^/?#]*)(/[^?#]*)?([?][^#]*)?([#].*)?$")
_ABS_PATH_NA = re.compile(r"^(/[^?#]*)?([?][^#]*)?([#].*)?$")
_OPAQUE_RE = re.compile(r"^([^?#]*)([?][^#]*)?([#].*)?$")


def _parse_authority(auth: str):
    """Return (userinfo, host, port) or None when malformed."""
    userinfo = None
    hostpart = auth
    at = auth.rfind("@")
    if at >= 0:
        userinfo = auth[:at]
        hostpart = auth[at + 1:]
    if hostpart.startswith("["):
        close = hostpart.find("]")
        if close < 0:
            return None
        host = hostpart[:close + 1]
        rest = hostpart[close + 1:]
        if rest and not rest.startswith(":"):
            return None
        return userinfo, host, rest.lstrip(":")
    if ":" in hostpart:
        host, _, port = hostpart.partition(":")
        if ":" in port:
            return None
        return userinfo, host, port
    return userinfo, hostpart, None


def _url_parts(url: str):
    """Parse per RFC 3986 into (scheme, userinfo, host, port, path,
    query, ref, opaque).  Mirrors the Java URI behavior observed in the
    pinned cell set: illegal characters (space, etc.) make the whole
    parse fail; case of scheme/host is preserved."""
    if url is None:
        return None
    if not url:
        return "", None, None, None, "", None, None, False
    if any(ord(c) < 0x21 or c in "\"<>\\^`{|}" for c in url):
        return None
    m = _SCHEME_RE.match(url)
    scheme = None
    rest = url
    if m:
        scheme = m.group(1)
        rest = url[m.end():]
        if not rest.startswith("//"):
            q = _OPAQUE_RE.match(rest)
            query = q.group(2)[1:] if q is not None and q.group(2) else None
            ref = q.group(3)[1:] if q is not None and q.group(3) else None
            return scheme, None, None, None, None, query, ref, True
    if rest.startswith("//"):
        m = _ABS_PATH_AT.match(rest)
        if not m:
            return None
        auth = m.group(2)
        path = m.group(3) if m.group(3) is not None else ""
        query = m.group(4)[1:] if m.group(4) else None
        ref = m.group(5)[1:] if m.group(5) else None
        if " " in auth:
            return None
        parsed = _parse_authority(auth)
        if not parsed:
            return None
        userinfo, host, port = parsed
        return scheme, userinfo, host, port, path, query, ref, False
    m = _ABS_PATH_NA.match(url)
    if not m:
        return None
    path = m.group(1) if m.group(1) is not None else ""
    query = m.group(2)[1:] if m.group(2) else None
    ref = m.group(3)[1:] if m.group(3) else None
    if path is None:
        return None
    return scheme, None, None, None, path, query, ref, False


def parse_url(url, part, key=None):
    """PySpark ``parse_url`` semantics (3.5.8 pin matrix, 168 cells).

    ``part`` is one of PROTOCOL/HOST/PATH/QUERY/REF/FILE/AUTHORITY/
    USERINFO; with ``key`` given the QUERY value for that parameter is
    returned (URLDecoder-style ``%XX`` and ``+`` decoding, pinned cells
    only cover absent-key/absent-query which yield NULL).  Case is
    preserved (``HTTP`` -> ``HTTP``).  Malformed URLs yield NULL for
    every part; the empty string URL yields ``''`` for PATH and FILE and
    NULL elsewhere.
    """
    if url is None:
        return None
    p = _url_parts(str(url))
    if p is None:
        return None
    scheme, userinfo, host, port, path, query, ref, opaque = p
    if part == "PROTOCOL":
        return scheme
    if part == "HOST":
        return host
    if part == "USERINFO":
        return userinfo
    if part == "AUTHORITY":
        if host is None:
            return None
        auth = host if port is None else host + ":" + port
        if userinfo is not None:
            auth = userinfo + "@" + auth
        return auth
    if part == "PATH":
        return path
    if part == "REF":
        return ref
    if part == "QUERY":
        if key is None:
            return query
        if query is None:
            return None
        for pair in query.split("&"):
            k, _, v = pair.partition("=")
            if _url_decode(k) == key:
                return _url_decode(v)
        return None
    if part == "FILE":
        if path is None:
            return None
        file = path
        if query is not None:
            file += "?" + query
        return file
    return None


def _url_decode(s: str) -> str:
    return re.sub(r"\+", " ", re.sub(r"%([0-9A-Fa-f]{2})", lambda m: chr(int(m.group(1), 16)), s))


# ---------------------------------------------------------------------------
# timezone functions
# ---------------------------------------------------------------------------

_TS_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$")


def _parse_ts(ts: str):
    m = _TS_RE.match(ts)
    if not m:
        raise ValueError("Cannot parse timestamp: " + repr(ts))
    us = m.group(7)
    return (
        int(m.group(1)), int(m.group(2)), int(m.group(3)),
        int(m.group(4)), int(m.group(5)), int(m.group(6)),
        int(us.ljust(6, "0")) if us else 0,
    )


def _zone(tz: str):
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        return ZoneInfo(tz)
    except ZoneInfoNotFoundError:
        m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})?", tz)
        if m:
            off = timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0))
            if m.group(1) == "-":
                off = -off
            return timezone(off)
        raise


def _make_dt(y, mo, d, h, mi, s, us, tzinfo):
    return datetime(y, mo, d, h, mi, s, us, tzinfo=tzinfo)


def _fmt_dt(dt: datetime) -> str:
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.microsecond:
        base += f".{dt.microsecond:06d}"
    return base


def from_utc_timestamp(ts, tz):
    """PySpark ``from_utc_timestamp`` semantics: interpret ``ts`` as UTC
    and render the wall-clock time in ``tz``.  DST gaps use the
    pre-transition offset, overlaps use the earlier offset (fold=0),
    i.e. zoneinfo's default.  Offsets ``+HH:MM`` are accepted;
    unknown zones raise (a SQL error, matching Spark's throw).
    Input microseconds are preserved."""
    if ts is None or tz is None:
        return None
    y, mo, d, h, mi, s, us = _parse_ts(str(ts))
    z = _zone(str(tz))
    local = _make_dt(y, mo, d, h, mi, s, us, timezone.utc).astimezone(z)
    return _fmt_dt(local)


def to_utc_timestamp(ts, tz):
    """PySpark ``to_utc_timestamp`` semantics: interpret ``ts`` as wall
    clock in ``tz`` and render the equivalent UTC time."""
    if ts is None or tz is None:
        return None
    y, mo, d, h, mi, s, us = _parse_ts(str(ts))
    z = _zone(str(tz))
    utc = _make_dt(y, mo, d, h, mi, s, us, z).astimezone(timezone.utc)
    return _fmt_dt(utc)


__all__ = [
    "conv", "format_string", "printf", "parse_url",
    "from_utc_timestamp", "to_utc_timestamp",
]
