from numpy import NAN
import pandas as pd
import numpy as np
s = pd.Series([1231.414, 414.1424, 1414.31, None])
print(s[3])
print(np.isnan(s[3]))