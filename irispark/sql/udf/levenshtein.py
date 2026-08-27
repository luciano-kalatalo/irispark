DDL = r"""
CREATE OR REPLACE FUNCTION levenshtein(str1 VARCHAR(4000), str2 VARCHAR(4000))
RETURNS INT
LANGUAGE OBJECTSCRIPT
{
    Set len1 = $LENGTH(str1)
    Set len2 = $LENGTH(str2)
    If len1 = 0 { Quit len2 }
    If len2 = 0 { Quit len1 }
    Set v1(0) = 0
    Set j = 1
    While j '= (len2 + 1) {
        Set v1(j) = j
        Set j = j + 1
    }
    Set i = 1
    While i '= (len1 + 1) {
        Set v2(0) = i
        Set j = 1
        While j '= (len2 + 1) {
            If $EXTRACT(str1, i) = $EXTRACT(str2, j) {
                Set cost = 0
            } Else {
                Set cost = 1
            }
            Set d = v2(j - 1) + 1
            Set m = v1(j) + 1
            If m < d { Set d = m }
            Set m = v1(j - 1) + cost
            If m < d { Set d = m }
            Set v2(j) = d
            Set j = j + 1
        }
        Set j = 0
        While j '= (len2 + 1) {
            Set v1(j) = v2(j)
            Set j = j + 1
        }
        Set i = i + 1
    }
    Quit v2(len2)
}
"""


def install(session):
    session.sql(DDL)
