DDL = r"""
CREATE OR REPLACE FUNCTION irispark_median(csv VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set total = $LENGTH(csv, ",")
    If total = 0 { Quit 0 }
    Set n = 0
    Set i = 1
    While i <= total {
        Set val = +$PIECE(csv, ",", i)
        Set n = n + 1
        Set vals(n) = val
        Set i = i + 1
    }
    If n = 0 { Quit 0 }
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
    Set mid = n \ 2
    If (n # 2) = 1 {
        Quit vals(mid + 1)
    }
    Quit (vals(mid) + vals(mid + 1)) / 2.0
}
"""


def install(session):
    session.sql(DDL)
