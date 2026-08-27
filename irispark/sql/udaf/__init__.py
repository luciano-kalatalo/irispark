from . import corr, distribution, extrema, moments

ALL = [corr, moments, distribution, extrema]


def install_all(session):
    for mod in ALL:
        mod.install(session)
