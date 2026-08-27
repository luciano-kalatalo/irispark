"""IRISPARK_MONTHS_BETWEEN — PySpark months_between semantics.

Derived from live PySpark 3.5.8 ground truth (scripts/parity.py vector set):

    months = (y2 - y1) * 12 + (m2 - m1)
    if both dates are the last day of their month: result = months (exact)
    elif end_day >= start_day: months + (d2 - d1) / 31.0
    else: months - 1 + (d2 + 31 - d1) / 31.0

The day-fraction denominator is a constant 31.0 (month lengths are NOT
used -- verified: ('2024-02-29','2024-02-28') -> -0.03225806 = -1/31, and
('2024-01-31','2024-02-28')[leap] -> 0.90322581 = 28/31). Timestamps are
reduced to their calendar date. NULL -> NULL.

Output rounding: Spark's observable months_between result is rounded
HALF-UP to 8 decimals (probed: computed 0.9032258064516129 -> returned
0.90322581, and 30/31 -> 0.96774194), so this UDF applies the same
rounding. The both-last special case applies in BOTH roundOff modes
(verified identical), so roundOff is not a parameter.

timestampdiff(MONTH/YEAR/QUARTER) rides on this helper with CAST truncation
(SELECT IRISPARK_MONTHS_BETWEEN(start, end) AS INTEGER), matching Spark's
truncated month counts.
"""

DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION irispark_months_between(start VARCHAR(40), end VARCHAR(40))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If (start = "") || (end = "") { Quit "" }
    Set d1 = $PIECE($PIECE(start, " ", 1), ")", 1)
    Set d2 = $PIECE($PIECE(end, " ", 1), ")", 1)
    Set ys = $PIECE(d1, "-", 1), ms = +$PIECE(d1, "-", 2), ds = +$PIECE(d1, "-", 3)
    Set ye = $PIECE(d2, "-", 1), me = +$PIECE(d2, "-", 2), de = +$PIECE(d2, "-", 3)
    Set months = (ye - ys) * 12 + (me - ms)
    Set dims = $SELECT(ms = 2: 28 + ((ys # 4 = 0) && ((ys # 100 '= 0) || (ys # 400 = 0))),
                       (ms = 4) || (ms = 6) || (ms = 9) || (ms = 11): 30,
                       1: 31)
    Set dime = $SELECT(me = 2: 28 + ((ye # 4 = 0) && ((ye # 100 '= 0) || (ye # 400 = 0))),
                       (me = 4) || (me = 6) || (me = 9) || (me = 11): 30,
                       1: 31)
    If (ds = dims) && (de = dime) {
        Set r = $DOUBLE(months)
    } ElseIf de >= ds {
        Set r = $DOUBLE(months) + ((de - ds) / 31)
    } Else {
        Set r = $DOUBLE(months) - 1 + ((de + 31 - ds) / 31)
    }
    Set r8 = $DOUBLE(r) * 100000000
    Set sgn = 1
    If r8 < 0 { Set sgn = -1, r8 = -r8 }
    Set r8 = ((r8 + 0.5) \ 1) * sgn
    Set r8 = r8 / 100000000
    Quit r8
}
""",
]


def install(session):
    for stmt in DDL_STATEMENTS:
        session.sql(stmt)
