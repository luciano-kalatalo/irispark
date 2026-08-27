DDL = r"""
CREATE OR REPLACE FUNCTION irispark_regexp_replace(
    str VARCHAR(4000), pattern VARCHAR(4000), replacement VARCHAR(4000)
)
RETURNS VARCHAR(4000)
LANGUAGE OBJECTSCRIPT
{
    Set m = ##class(%Regex.Matcher).%New(pattern, str)
    Quit m.ReplaceAll(replacement)
}
"""


def install(session):
    session.sql(DDL)
