from hikyuu import *
import pandas as pd
s = pd.Series([1,2,3,4,5], dtype=float)
ind = PRICELIST(s.tolist())
ma = MA(ind, 3)
print('ind size', len(ind))
print('ma', ma)
print('ma list', [ma[i] for i in range(len(ma))])
