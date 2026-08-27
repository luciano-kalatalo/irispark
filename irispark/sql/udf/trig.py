"""IRISPARK_ASIN / IRISPARK_ACOS — arc-trig with PySpark NaN semantics.

Native IRIS ASIN/ACOS raise a FATAL SQLCODE -400 (ILLEGAL VALUE) on
out-of-domain input (verified: SELECT ASIN(2) on 2026.2), while PySpark
returns NaN, and NULL propagates NULL. These guarded helpers return
$DOUBLE("nan") out of domain. ATAN/ATAN2 are domain-safe for all reals
and stay native SQL in functions.py.

Name probe (2026.2): the ObjectScript arc-trig functions are
$ZARCSIN / $ZARCCOS (the $ZASIN/$ZACOS spellings do not compile).
"""

DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION irispark_asin(x DOUBLE)
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If x = "" { Quit "" }
    If (x > 1) || (x < -1) { Quit $DOUBLE("nan") }
    Quit $ZARCSIN(x)
}
""",
    r"""
CREATE OR REPLACE FUNCTION irispark_acos(x DOUBLE)
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If x = "" { Quit "" }
    If (x > 1) || (x < -1) { Quit $DOUBLE("nan") }
    Quit $ZARCCOS(x)
}
""",
]


def install(session):
    for stmt in DDL_STATEMENTS:
        session.sql(stmt)
