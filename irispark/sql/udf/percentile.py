DDL = r"""
CREATE OR REPLACE FUNCTION irispark_percentile(csv VARCHAR(4000), percentage DOUBLE)
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set total = $LENGTH(csv, ",")
    If total = 0 { Quit 0 }
    If percentage < 0 { Set percentage = 0 }
    If percentage > 1 { Set percentage = 1 }
    Set n = 0
    Set i = 1
    While i <= total {
        Set val = +$PIECE(csv, ",", i)
        Set n = n + 1
        Set vals(n) = val
        Set i = i + 1
    }
    If n = 0 { Quit 0 }
    If n = 1 { Quit vals(1) }
    Set i = 1
    While i < n {
        Set j = i + 1
        While j <= n {
            If vals(i) > vals(j) {
                Set tmp = vals(i)
                Set vals(i) = vals(j)
                Set vals(j) = tmp
            }
            Set j = j + 1
        }
        Set i = i + 1
    }
    Set pos = (n - 1) * percentage
    Set pos = 1 + pos
    Set lo = pos \ 1
    Set hi = lo + 1
    If lo < 1 { Set lo = 1 }
    If hi > n { Set hi = n }
    If lo = hi { Quit vals(lo) }
    Set frac = pos - lo
    Set a = vals(lo) * (1 - frac)
    Set b = vals(hi) * frac
    Quit a + b
}
"""


def install(session):
    session.sql(DDL)
