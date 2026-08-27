DDL = r"""
CREATE OR REPLACE FUNCTION irispark_kurtosis(csv VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set total = $LENGTH(csv, ",")
    If total < 4 { Quit 0 }
    Set n = 0
    Set s1 = 0
    Set s2 = 0
    Set s3 = 0
    Set s4 = 0
    Set i = 1
    While i <= total {
        Set val = +$PIECE(csv, ",", i)
        Set n = n + 1
        Set s1 = s1 + val
        Set s2 = s2 + (val * val)
        Set s3 = s3 + (val * val * val)
        Set s4 = s4 + (val * val * val * val)
        Set i = i + 1
    }
    Set mean = s1 / n
    Set ss = s2 - ((s1 * s1) / n)
    Set var = ss / (n - 1)
    If var = 0 { Quit 0 }
    Set sd = var ** 0.5
    Set m4 = s4 - (4 * mean * s3) + (6 * (mean * mean) * s2) - (4 * (mean * mean * mean) * s1) + (n * (mean * mean * mean * mean))
    Set g2a = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))
    Set g2b = m4 / n
    Set g2c = sd * sd * sd * sd
    If g2c = 0 { Quit 0 }
    Set g2num = g2a * g2b / g2c
    Set g2den = 3 * (n - 1) * (n - 1) / ((n - 2) * (n - 3))
    Set g2 = g2num - g2den
    Quit g2
}
"""


def install(session):
    session.sql(DDL)
