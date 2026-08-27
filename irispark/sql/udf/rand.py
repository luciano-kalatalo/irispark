DDL = r"""
CREATE OR REPLACE FUNCTION irispark_rand(seed INT)
RETURNS DOUBLE
LANGUAGE OBJECTSCRIPT
{
    Set r = $RANDOM(1000000) + seed
    Quit (r # 1000000) / 1000000.0
}
"""


def install(session):
    session.sql(DDL)
