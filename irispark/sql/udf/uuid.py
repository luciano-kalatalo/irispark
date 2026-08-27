DDL = r"""
CREATE OR REPLACE FUNCTION uuid()
RETURNS VARCHAR(36)
LANGUAGE OBJECTSCRIPT
{
    Quit $SYSTEM.Util.CreateGUID()
}
"""


def install(session):
    session.sql(DDL)
