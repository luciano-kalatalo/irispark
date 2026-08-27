"""IRISPARK.MEDIAN / IRISPARK.PERCENTILE / IRISPARK.QUANTILE UDAFs.

State: pipe-joined "p|v1|v2|..." — the probability p followed by every
observed value (NULLs skipped). FINALIZE computes the pandas-style linear
interpolation quantile (pandas `quantile()` default, SQL PERCENTILE_CONT
semantics): the k-th order statistics around (n-1)*p are located with a
single Lomuto quickselect (the tail is then entirely >= v1, so v2 is the
tail minimum), and the result interpolates linearly between them.

Reference implementation with a hard single-group ceiling: the state
grows O(n) per group (pipe-joined, carried in a VARCHAR(4000)); past
~250k values the state exceeds IRIS's MAXSTRING (~3.6MB) and the per-row
concat in ITERATE fails fatally with SQLCODE -400 (verified empirically,
not a graceful CLOB promotion). The PRODUCTION path is the analytic engine
expanded by the SQL generator (functions.median/percentile — no
state-size limits); these UDAFs are the reference/fallback below the
ceiling (validated up to 100k values in one group; see
rules_functions_aggregations.md section 6.1).
"""

DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION irispark_pct_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit ""
}
""",
    r"""
CREATE OR REPLACE FUNCTION irispark_pct_iter(state VARCHAR(4000), x DOUBLE, p DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If x = "" { Quit state }
    If p = "" { Quit state }
    If state = "" { Quit p _ "|" _ x }
    Quit state _ "|" _ x
}
""",
    r"""
CREATE OR REPLACE FUNCTION irispark_median_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0.5"
}
""",
    r"""
CREATE OR REPLACE FUNCTION irispark_median_iter(state VARCHAR(4000), x DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If x = "" { Quit state }
    Quit state _ "|" _ x
}
""",
    r"""
CREATE OR REPLACE FUNCTION irispark_pct_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set p = +$DOUBLE($PIECE(state, "|", 1))
    If p < 0 { Quit "" }
    If p > 1 { Quit "" }
    Set n = $LENGTH(state, "|") - 1
    If n = 0 { Quit "" }
    Set vals = ""
    Set i = 2
    Set m = n + 1
    While i <= m {
        Set $LIST(vals, i - 1) = +$DOUBLE($PIECE(state, "|", i))
        Set i = i + 1
    }
    If n = 1 { Quit +$LIST(vals, 1) }
    Set pos = (n - 1) * p
    Set klo = +$PIECE(pos, ".", 1) + 1
    Set khi = klo + 1
    Set frac = pos - (klo - 1)
    Set v1 = 0
    Set v2 = 0
    Set k = klo
    Set lo = 1
    Set hi = n
    Set found = 0
    While found = 0 {
        Set pivot = +$LIST(vals, hi)
        Set lt = lo
        Set gt = hi
        Set i = lo
        While i <= gt {
            Set val = +$LIST(vals, i)
            If val < pivot {
                Set tmp = +$LIST(vals, lt)
                Set $LIST(vals, lt) = val
                Set $LIST(vals, i) = tmp
                Set lt = lt + 1
                Set i = i + 1
            } Else {
                If val > pivot {
                    Set tmp = +$LIST(vals, gt)
                    Set $LIST(vals, gt) = val
                    Set $LIST(vals, i) = tmp
                    Set gt = gt - 1
                } Else {
                    Set i = i + 1
                }
            }
        }
        If k <= gt {
            If k >= lt { Set v1 = pivot Set found = 1 }
            Else { Set hi = lt - 1 }
        } Else {
            Set lo = gt + 1
        }
    }
    If khi > n {
        Set v2 = v1
    } Else {
        Set i = klo + 1
        Set v2 = +$LIST(vals, i)
        While i <= n {
            If +$LIST(vals, i) < v2 { Set v2 = +$LIST(vals, i) }
            Set i = i + 1
        }
    }
    Set result = v1 + ((v2 - v1) * frac)
    Quit result
}
""",
]

DDL_AGG_DROP = [
    "DROP AGGREGATE IRISPARK.MEDIAN",
    "DROP AGGREGATE IRISPARK.PERCENTILE",
    "DROP AGGREGATE IRISPARK.QUANTILE",
]

DDL_AGG_CREATE = [
    """
CREATE AGGREGATE IRISPARK.MEDIAN(x DOUBLE)
    INITIALIZE WITH irispark_median_init
    ITERATE WITH irispark_median_iter
    FINALIZE WITH irispark_pct_final
""",
    """
CREATE AGGREGATE IRISPARK.PERCENTILE(x DOUBLE, p DOUBLE)
    INITIALIZE WITH irispark_pct_init
    ITERATE WITH irispark_pct_iter
    FINALIZE WITH irispark_pct_final
""",
    """
CREATE AGGREGATE IRISPARK.QUANTILE(x DOUBLE, p DOUBLE)
    INITIALIZE WITH irispark_pct_init
    ITERATE WITH irispark_pct_iter
    FINALIZE WITH irispark_pct_final
""",
]


def install(session):
    for stmt in DDL_STATEMENTS:
        session.sql(stmt)
    for stmt in DDL_AGG_DROP:
        try:
            session.sql(stmt)
        except Exception:
            pass
    for stmt in DDL_AGG_CREATE:
        session.sql(stmt)
