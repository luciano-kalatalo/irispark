from . import (
    crc32,
    datetime_ext,
    epython,
    initcap,
    kurtosis,
    levenshtein,
    md5,
    median,
    percentile,
    rand,
    regexp_extract,
    regexp_replace,
    sha1,
    sha2,
    skewness,
    soundex,
    split,
    trig,
    uuid,
)

ALL = [md5, sha1, sha2, crc32, initcap, uuid,
        levenshtein, soundex, regexp_extract, median, percentile,
        regexp_replace, split, rand, skewness, kurtosis, trig,
        datetime_ext, epython]


def install_all(session):
    for mod in ALL:
        mod.install(session)
