def percentage(x):
    return "{0:.2f}%".format(100*float(x)).replace("nan%", "")


def fdouble(x):
    return "{0:.2f}".format(float(x)).replace("nan", "")


def fint(x):
    try:
        return "{:d}".format(int(x))
    except:
        return ""