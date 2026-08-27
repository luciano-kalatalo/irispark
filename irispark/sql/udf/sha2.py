DDL = r"""
CREATE OR REPLACE FUNCTION sha2(str VARCHAR(4000), bits INT)
RETURNS VARCHAR(128)
LANGUAGE OBJECTSCRIPT
{
    Set hash = ##class(%SYSTEM.Encryption).SHAHash(bits, str)
    Set result = ""
    Set len = $LENGTH(hash)
    Set i = 1
    While i <= len {
        Set byte = $ASCII(hash,i)
        Set hi = byte \ 16
        Set lo = byte # 16
        Set result = result _ $EXTRACT("0123456789abcdef", hi+1) _ $EXTRACT("0123456789abcdef", lo+1)
        Set i = i + 1
    }
    Quit result
}
"""


def install(session):
    session.sql(DDL)
