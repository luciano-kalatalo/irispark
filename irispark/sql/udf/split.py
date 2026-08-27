DDL = r"""
CREATE OR REPLACE FUNCTION irispark_split(
    str VARCHAR(4000), pattern VARCHAR(4000)
)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Set result = ""
    Set m = ##class(%Regex.Matcher).%New(pattern, str)
    Set pos = 1
    While m.Locate(pos) {
        If result '= "" { Set result = result _ "," }
        Set end = m.Start - 1
        If end >= pos { Set result = result _ $EXTRACT(str, pos, end) }
        Set pos = m.Start + $LENGTH(m.Group)
    }
    If pos <= $LENGTH(str) {
        If result '= "" { Set result = result _ "," }
        Set result = result _ $EXTRACT(str, pos, $LENGTH(str))
    }
    Quit result
}
"""


def install(session):
    session.sql(DDL)
