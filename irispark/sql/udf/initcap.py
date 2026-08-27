DDL = r"""
CREATE OR REPLACE FUNCTION initcap(str VARCHAR(4000))
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Quit $ZCONVERT(str, "W")
}
"""


def install(session):
    session.sql(DDL)
