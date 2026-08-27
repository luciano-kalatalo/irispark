"""IRISPARK.AGG_FIRST / AGG_LAST / AGG_MAX_BY / AGG_MIN_BY ObjectScript UDAFs.

State for FIRST/LAST: "0|" (empty) or "1|<value>".
State for MAX_BY/MIN_BY: "1|<key y>|<value x>"; ITERATE keeps the pair with
the strictly better key (ties keep the first seen row).

MERGE only for MAX_BY/MIN_BY: keep the pair with the better carried key,
left state winning ties — identical to sequential first-seen semantics.
FIRST/LAST are order-dependent by definition, so they carry NO merge: IRIS
would still run them single-threaded under %PARALLEL, which is correct
behavior for them (a merged first/last would be nondeterministic).
"""

DDL_STATEMENTS = [
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.first_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0|"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.first_iter(state VARCHAR(4000), x DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Set f = +$PIECE(state, "|", 1)
    If (f = 0) && (x '= "") { Quit "1|"_$DOUBLE(x) }
    Quit state
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.first_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If +$PIECE(state, "|", 1) = 0 { Quit "" }
    Quit $DOUBLE($PIECE(state, "|", 2))
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.last_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0|"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.last_iter(state VARCHAR(4000), x DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If x '= "" { Quit "1|"_$DOUBLE(x) }
    Quit state
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.last_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If +$PIECE(state, "|", 1) = 0 { Quit "" }
    Quit $DOUBLE($PIECE(state, "|", 2))
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.max_by_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0||"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.max_by_iter(state VARCHAR(4000), x DOUBLE, y DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If (x = "") || (y = "") { Quit state }
    Set h   = +$PIECE(state, "|", 1)
    Set by  = +$PIECE(state, "|", 2)
    If (h = 0) || (by < +y) { Quit "1|"_$DOUBLE(+y)_"|"_$DOUBLE(+x) }
    Quit state
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.max_by_merge(state1 VARCHAR(4000), state2 VARCHAR(4000))
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    New h1, by1, h2, by2

    Set h1 = +$PIECE(state1, "|", 1)
    Set h2 = +$PIECE(state2, "|", 1)
    If h2 = 0 { Quit state1 }
    If h1 = 0 { Quit state2 }

    Set by1 = +$PIECE(state1, "|", 2)
    Set by2 = +$PIECE(state2, "|", 2)
    If by2 > by1 { Quit state2 }
    Quit state1
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.max_by_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If +$PIECE(state, "|", 1) = 0 { Quit "" }
    Quit $DOUBLE($PIECE(state, "|", 3))
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.min_by_init()
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit "0||"
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.min_by_iter(state VARCHAR(4000), x DOUBLE, y DOUBLE)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If (x = "") || (y = "") { Quit state }
    Set h   = +$PIECE(state, "|", 1)
    Set by  = +$PIECE(state, "|", 2)
    If (h = 0) || (by > +y) { Quit "1|"_$DOUBLE(+y)_"|"_$DOUBLE(+x) }
    Quit state
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.min_by_merge(state1 VARCHAR(4000), state2 VARCHAR(4000))
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    New h1, by1, h2, by2

    Set h1 = +$PIECE(state1, "|", 1)
    Set h2 = +$PIECE(state2, "|", 1)
    If h2 = 0 { Quit state1 }
    If h1 = 0 { Quit state2 }

    Set by1 = +$PIECE(state1, "|", 2)
    Set by2 = +$PIECE(state2, "|", 2)
    If by2 < by1 { Quit state2 }
    Quit state1
}
""",
    r"""
CREATE OR REPLACE FUNCTION IRISPARK.min_by_final(state VARCHAR(4000))
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    If +$PIECE(state, "|", 1) = 0 { Quit "" }
    Quit $DOUBLE($PIECE(state, "|", 3))
}
""",
]

DDL_AGG_CREATE = [
    """
CREATE OR REPLACE AGGREGATE IRISPARK.AGG_FIRST(x DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.first_init
    ITERATE WITH IRISPARK.first_iter
    FINALIZE WITH IRISPARK.first_final
""",
    """
CREATE OR REPLACE AGGREGATE IRISPARK.AGG_LAST(x DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.last_init
    ITERATE WITH IRISPARK.last_iter
    FINALIZE WITH IRISPARK.last_final
""",
    """
CREATE OR REPLACE AGGREGATE IRISPARK.AGG_MAX_BY(x DOUBLE, y DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.max_by_init
    ITERATE WITH IRISPARK.max_by_iter
    MERGE WITH IRISPARK.max_by_merge
    FINALIZE WITH IRISPARK.max_by_final
""",
    """
CREATE OR REPLACE AGGREGATE IRISPARK.AGG_MIN_BY(x DOUBLE, y DOUBLE)
    RETURNS DOUBLE
    INITIALIZE WITH IRISPARK.min_by_init
    ITERATE WITH IRISPARK.min_by_iter
    MERGE WITH IRISPARK.min_by_merge
    FINALIZE WITH IRISPARK.min_by_final
""",
]

OLD_HELPERS = [
    "SQLUSER.irispark_first_init",
    "SQLUSER.irispark_first_iter",
    "SQLUSER.irispark_first_final",
    "SQLUSER.irispark_last_init",
    "SQLUSER.irispark_last_iter",
    "SQLUSER.irispark_last_final",
    "SQLUSER.irispark_max_by_init",
    "SQLUSER.irispark_max_by_iter",
    "SQLUSER.irispark_max_by_final",
    "SQLUSER.irispark_min_by_init",
    "SQLUSER.irispark_min_by_iter",
    "SQLUSER.irispark_min_by_final",
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
