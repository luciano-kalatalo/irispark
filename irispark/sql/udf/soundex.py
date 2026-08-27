DDL = r"""
CREATE OR REPLACE FUNCTION soundex(str VARCHAR(4000))
RETURNS VARCHAR(4)
LANGUAGE OBJECTSCRIPT
{
    If $LENGTH(str) = 0 { Quit "0000" }
    Set str = $ZCONVERT(str, "U")
    Set first = $EXTRACT(str, 1)
    Set result = first
    Set prev = 0
    Set mapping = "01230120022455012623010202"
    Set i = 2
    While i <= $LENGTH(str) {
        Set ch = $EXTRACT(str, i)
        Set pos = $ASCII(ch) - 64
        If (pos >= 1) && (pos <= 26) {
            Set code = $EXTRACT(mapping, pos)
            If (code '= "0") && (code '= prev) {
                Set result = result _ code
                Set prev = code
            }
        }
        Set i = i + 1
    }
    Set result = $EXTRACT(result, 1, 4)
    While $LENGTH(result) < 4 {
        Set result = result _ "0"
    }
    Quit result
}
"""


def install(session):
    session.sql(DDL)
