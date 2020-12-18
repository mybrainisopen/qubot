import pandas as pd
import numpy as np
df = pd.DataFrame({"A":[1,2,3], "B":[4,5,6], "C":[7,8,9]})
print(df)

df.iloc[1][1:] = df.iloc[1][1:] * 7
print(df)