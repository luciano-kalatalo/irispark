DDL = r"""
CREATE OR REPLACE FUNCTION regexp_extract(str VARCHAR(4000), pattern VARCHAR(4000), idx INT)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    If str = "" { Quit "" }
    Set m = ##class(%Regex.Matcher).%New(pattern, str)
    Set result = ""
    If m.Locate() {
        Set count = m.GroupCount
        If idx <= count { Set result = m.Group(idx) }
    }
    Quit result
}
"""


def install(session):
    session.sql(DDL)
