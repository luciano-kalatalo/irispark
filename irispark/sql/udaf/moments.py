"""IRISPARK.SKEWNESS / IRISPARK.KURTOSIS ObjectScript UDAFs.

State: pipe-joined "n|mean|M2|M3|M4" — raw central-moment SUMS updated via
the Pébay online algorithm (numerically stable, mergeable). FINALIZE applies
pandas-exact semantics: skewness = sqrt(n(n-1))/(n-2) * (M3/n)/(M2/n)^1.5 and
kurtosis = n(n+1)(n-1)M4/((n-2)(n-3)M2^2) - 3(n-1)^2/((n-2)(n-3))
(matching pandas Series.skew/kurt on the same columns); NULL when n is too
small, and 0 when variance is zero (pandas fperr parity).

MERGE (Pébay parallel combination, both aggregates): given
(na, meanA, M2A, M3A, M4A) and (nb, meanB, M2B, M3B, M4B):

    n   = na + nb
    d   = meanB - meanA
    mean = meanA + d * nb / n
    M2  = M2A + M2B + d^2 * na*nb/n
    M3  = M3A + M3B + d^3 * na*nb*(na-nb)/n^2
        + 3*d*(na*M2B - nb*M2A)/n
    M4  = M4A + M4B + d^4 * na*nb*(na^2 - na*nb + nb^2)/n^3
        + 6*d^2*(na^2*M2B + nb^2*M2A)/n^2
        + 4*d*(na*M3B - nb*M3A)/n

Pinned by tests/test_udaf_moments.py (python-side Pébay emulation vs
IRISPARK.mom_merge, %PARALLEL == serial smoke, 1e9-scale stress).

Reference implementation: state is fixed-size (independent of row count);
see the governance doc for the performance trade-off vs expanded SQL.
"""

DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.mom_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0|0|0|0|0"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.mom_iter(state VARCHAR(4000), x DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If x = "" { Quit state }
    Set n0 = +$DOUBLE($PIECE(state, "|", 1))
    Set mn = +$DOUBLE($PIECE(state, "|", 2))
    Set m2 = +$DOUBLE($PIECE(state, "|", 3))
    Set m3 = +$DOUBLE($PIECE(state, "|", 4))
    Set m4 = +$DOUBLE($PIECE(state, "|", 5))
    Set n = n0 + 1
    Set d = x - mn
    Set dn = d / n
    Set dn2 = dn * dn
    Set dn3 = dn2 * dn
    Set dn4 = dn3 * dn
    Set a1 = dn * m3
    Set a2 = a1 * 4
    Set b1 = dn2 * m2
    Set b2 = b1 * 6
    Set c1 = dn4 * n0
    Set c2 = n0 * n0
    Set c3 = c2 * n0
    Set c4 = c3 + 1
    Set c5 = c1 * c4
    Set m4 = m4 - a2 + b2 + c5
    Set d1 = dn3 * n0
    Set d2 = n0 * n0
    Set d3 = d2 - 1
    Set d4 = d1 * d3
    Set e1 = dn * m2
    Set e2 = e1 * 3
    Set m3 = m3 + d4 - e2
    Set f1 = d * dn
    Set f2 = f1 * n0
    Set m2 = m2 + f2
    Set mn2 = mn + dn
    Quit n _ "|" _ mn2 _ "|" _ m2 _ "|" _ m3 _ "|" _ m4
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.mom_merge(state1 VARCHAR(4000), state2 VARCHAR(4000))
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    New na, mna, m2a, m3a, m4a
    New nb, mnb, m2b, m3b, m4b
    New n, d, mn, m2, m3, m4
    New d2, d3, d4, nab
    New t1, t2, u1, u2, u3

    Set nb = +$DOUBLE($PIECE(state2, "|", 1))
    If nb = 0 { Quit state1 }
    Set na = +$DOUBLE($PIECE(state1, "|", 1))
    If na = 0 { Quit state2 }

    Set mna = +$DOUBLE($PIECE(state1, "|", 2))
    Set m2a = +$DOUBLE($PIECE(state1, "|", 3))
    Set m3a = +$DOUBLE($PIECE(state1, "|", 4))
    Set m4a = +$DOUBLE($PIECE(state1, "|", 5))
    Set mnb = +$DOUBLE($PIECE(state2, "|", 2))
    Set m2b = +$DOUBLE($PIECE(state2, "|", 3))
    Set m3b = +$DOUBLE($PIECE(state2, "|", 4))
    Set m4b = +$DOUBLE($PIECE(state2, "|", 5))

    Set n   = na + nb
    Set d   = mnb - mna
    Set mn  = mna + (($DOUBLE(d) * $DOUBLE(nb)) / $DOUBLE(n))

    Set d2  = $DOUBLE(d) * $DOUBLE(d)
    Set d3  = d2 * $DOUBLE(d)
    Set d4  = d3 * $DOUBLE(d)
    Set nab = ($DOUBLE(na) * $DOUBLE(nb)) / $DOUBLE(n)

    Set m2 = m2a + m2b + ($DOUBLE(d2) * $DOUBLE(nab))

    Set t1 = ($DOUBLE(d3) * ($DOUBLE(na) * $DOUBLE(nb)) * ($DOUBLE(na) - $DOUBLE(nb))) / ($DOUBLE(n) * $DOUBLE(n))
    Set t2 = ($DOUBLE(3) * $DOUBLE(d) * (($DOUBLE(na) * m2b) - ($DOUBLE(nb) * m2a))) / $DOUBLE(n)
    Set m3 = m3a + m3b + t1 + t2

    Set u1 = ($DOUBLE(d4) * ($DOUBLE(na) * $DOUBLE(nb)) * (($DOUBLE(na) * $DOUBLE(na)) - ($DOUBLE(na) * $DOUBLE(nb)) + ($DOUBLE(nb) * $DOUBLE(nb)))) / ($DOUBLE(n) * $DOUBLE(n) * $DOUBLE(n))
    Set u2 = ($DOUBLE(6) * d2 * (($DOUBLE(na) * $DOUBLE(na) * m2b) + ($DOUBLE(nb) * $DOUBLE(nb) * m2a))) / ($DOUBLE(n) * $DOUBLE(n))
    Set u3 = ($DOUBLE(4) * $DOUBLE(d) * (($DOUBLE(na) * m3b) - ($DOUBLE(nb) * m3a))) / $DOUBLE(n)
    Set m4 = m4a + m4b + u1 + u2 + u3

    Quit n _ "|" _ mn _ "|" _ m2 _ "|" _ m3 _ "|" _ m4
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.skewness_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set n = +$DOUBLE($PIECE(state, "|", 1))
    If n < 3 { Quit "" }
    Set m2 = +$DOUBLE($PIECE(state, "|", 3))
    If m2 <= 0 { Quit 0 }
    Set m3 = +$DOUBLE($PIECE(state, "|", 4))
    Set s1 = m2 * (m2 ** 0.5)
    Set r = m3 / s1
    Set g1 = n * (n - 1)
    Set g2 = g1 ** 0.5
    Set g3 = n ** 0.5
    Set g = g2 * g3 / (n - 2)
    Quit $DOUBLE(r * g)
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.kurtosis_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set n = +$DOUBLE($PIECE(state, "|", 1))
    If n < 4 { Quit "" }
    Set m2 = +$DOUBLE($PIECE(state, "|", 3))
    If m2 <= 0 { Quit 0 }
    Set m4 = +$DOUBLE($PIECE(state, "|", 5))
    Set a1 = n * (n + 1)
    Set a2 = a1 * (n - 1)
    Set a3 = a2 * m4
    Set a4 = (n - 2) * (n - 3)
    Set a5 = a4 * m2
    Set a6 = a5 * m2
    Set a = a3 / a6
    Set b1 = n - 1
    Set b2 = b1 * b1
    Set b3 = b2 * 3
    Set b4 = (n - 2) * (n - 3)
    Set b = b3 / b4
    Quit $DOUBLE(a - b)
}
""",
]

DDL_AGG_CREATE = [
    """
CREATE OR REPLACE AGGREGATE IRISPARK.SKEWNESS(x DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.mom_init
    ITERATE WITH IRISPARK.mom_iter
    MERGE WITH IRISPARK.mom_merge
    FINALIZE WITH IRISPARK.skewness_final
""",
    """
CREATE OR REPLACE AGGREGATE IRISPARK.KURTOSIS(x DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.mom_init
    ITERATE WITH IRISPARK.mom_iter
    MERGE WITH IRISPARK.mom_merge
    FINALIZE WITH IRISPARK.kurtosis_final
""",
]

OLD_HELPERS = [
    "SQLUSER.irispark_mom_init",
    "SQLUSER.irispark_mom_iter",
    "SQLUSER.irispark_skewness_final",
    "SQLUSER.irispark_kurtosis_final",
]


def install(session):
    for stmt in DDL_STATEMENTS:
        session.sql(stmt)
    for stmt in DDL_AGG_CREATE:
        session.sql(stmt)
    for name in OLD_HELPERS:
        try:
            session.sql(f"DROP FUNCTION {name}")
        except Exception:
            pass
