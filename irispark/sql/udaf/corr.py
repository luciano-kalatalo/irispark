DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.corr_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0|0|0|0|0|0"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.corr_iter(state VARCHAR(4000), x DOUBLE, y DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    New n, mx, my, c, m2x, m2y, dxx, dyy

    If (x = "") || (y = "") { Quit state }

    Set n   = +$PIECE(state, "|", 1)
    Set mx  = +$PIECE(state, "|", 2)
    Set my  = +$PIECE(state, "|", 3)
    Set c   = +$PIECE(state, "|", 4)
    Set m2x = +$PIECE(state, "|", 5)
    Set m2y = +$PIECE(state, "|", 6)

    Set n   = n + 1
    Set dxx = $DOUBLE(x) - mx
    Set mx  = mx + ($DOUBLE(dxx) / $DOUBLE(n))
    Set dyy = $DOUBLE(y) - my
    Set my  = my + ($DOUBLE(dyy) / $DOUBLE(n))
    Set c   = c + ($DOUBLE(dxx) * ($DOUBLE(y) - my))
    Set m2x = m2x + ($DOUBLE(dxx) * ($DOUBLE(x) - mx))
    Set m2y = m2y + ($DOUBLE(dyy) * ($DOUBLE(y) - my))

    Quit n_"|"_mx_"|"_my_"|"_c_"|"_m2x_"|"_m2y
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.corr_merge(state1 VARCHAR(4000), state2 VARCHAR(4000))
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    New n1, mx1, my1, c1, m2x1, m2y1
    New n2, mx2, my2, c2, m2x2, m2y2
    New n, ddx, ddy, mx, my, c, m2x, m2y

    Set n2 = +$PIECE(state2, "|", 1)
    If n2 = 0 { Quit state1 }
    Set n1 = +$PIECE(state1, "|", 1)
    If n1 = 0 { Quit state2 }

    Set mx1  = +$PIECE(state1, "|", 2)
    Set my1  = +$PIECE(state1, "|", 3)
    Set c1   = +$PIECE(state1, "|", 4)
    Set m2x1 = +$PIECE(state1, "|", 5)
    Set m2y1 = +$PIECE(state1, "|", 6)
    Set mx2  = +$PIECE(state2, "|", 2)
    Set my2  = +$PIECE(state2, "|", 3)
    Set c2   = +$PIECE(state2, "|", 4)
    Set m2x2 = +$PIECE(state2, "|", 5)
    Set m2y2 = +$PIECE(state2, "|", 6)

    Set n   = n1 + n2
    Set ddx = mx2 - mx1
    Set ddy = my2 - my1
    Set mx  = mx1 + (($DOUBLE(ddx) * $DOUBLE(n2)) / $DOUBLE(n))
    Set my  = my1 + (($DOUBLE(ddy) * $DOUBLE(n2)) / $DOUBLE(n))
    Set c   = c1 + c2 + (($DOUBLE(ddx) * $DOUBLE(ddy) * $DOUBLE(n1) * $DOUBLE(n2)) / $DOUBLE(n))
    Set m2x = m2x1 + m2x2 + (($DOUBLE(ddx) * $DOUBLE(ddx) * $DOUBLE(n1) * $DOUBLE(n2)) / $DOUBLE(n))
    Set m2y = m2y1 + m2y2 + (($DOUBLE(ddy) * $DOUBLE(ddy) * $DOUBLE(n1) * $DOUBLE(n2)) / $DOUBLE(n))

    Quit n_"|"_mx_"|"_my_"|"_c_"|"_m2x_"|"_m2y
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.corr_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    New n, c, m2x, m2y, den, result

    Set n = +$PIECE(state, "|", 1)
    If n < 2 { Quit "" }

    Set c   = +$PIECE(state, "|", 4)
    Set m2x = +$PIECE(state, "|", 5)
    Set m2y = +$PIECE(state, "|", 6)

    If (m2x <= 0) || (m2y <= 0) { Quit "" }

    Set den    = ($DOUBLE(m2x) ** 0.5) * ($DOUBLE(m2y) ** 0.5)
    If den = 0 { Quit "" }

    Set result = $DOUBLE(c) / $DOUBLE(den)
    Quit result
}
""",
]

DDL_AGG_CREATE = r"""
CREATE OR REPLACE AGGREGATE IRISPARK.CORR(
    x DOUBLE,
    y DOUBLE
)
RETURNS DOUBLE
INITIALIZE WITH IRISPARK.corr_init
ITERATE WITH IRISPARK.corr_iter
MERGE WITH IRISPARK.corr_merge
FINALIZE WITH IRISPARK.corr_final
"""


def install(session):
    for stmt in DDL_STATEMENTS:
        session.sql(stmt)
    session.sql(DDL_AGG_CREATE)
