import pandas as pd
import math


def _safe(v, default=None):
    return v if (v is not None and v == v) else default

def sec_to_hms(s):
    s = 