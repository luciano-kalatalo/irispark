DDL = r"""
CREATE OR REPLACE FUNCTION crc32(str VARCHAR(4000))
RETURNS INT
LANGUAGE OBJECTSCRIPT
{
    If $ASCII(str) = 0 { Quit 0 }
    Quit $ZCRC(str, 7)
}
"""


def install(session):
    session.sql(DDL)
