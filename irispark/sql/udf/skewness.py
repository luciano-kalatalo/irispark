DDL = r"""
CREATE OR REPLACE FUNCTION irispark_skewness(csv VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set total = $LENGTH(csv, ",")
    If total < 3 { Quit 0 }
    Set n = 0
    Set s1 = 0
    Set s2 = 0
    Set s3 = 0
    Set i = 1
    While i <= total {
        Set val = +$PIECE(csv, ",", i)
        Set n = n + 1
        Set s1 = s1 + val
        Set s2 = s2 + (val * val)
        Set s3 = s3 + (val * val * val)
        Set i = i + 1
    }
    Set mean = s1 / n
    Set ss = s2 - ((s1 * s1) / n)
    Set var = ss / (n - 1)
    If var = 0 { Quit 0 }
    Set sd = var ** 0.5
    Set m3 = s3 - (3 * mean * s2) + (3 * (mean * mean) * s1) - (n * (mean * mean * mean))
    Set g1a = n / ((n - 1) * (n - 2))
    Set g1b = m3 / n
    Set g1c = sd * sd * sd
    If g1c = 0 { Quit 0 }
    Set g1 = g1a * g1b / g1c
    Quit g1
}
"""


def install(session):
    session.sql(DDL)
