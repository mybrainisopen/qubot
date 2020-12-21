import pandas as pd
import numpy as np
# df = pd.DataFrame({"A":[1,2,3], "B":[4,5,6], "C":[7,8,9]})
# print(df)
#
# df.iloc[1][1:] = df.iloc[1][1:] * 7
# print(df)

text = 'EVEBIT+PER+ROE+1MRM+F_SCORE>=9'
text = text.split('+')

for strtgy in text:
    if strtgy[0:7] in ['PER', 'PBR', 'PSR', 'PCR', 'PEG', 'EVEBIT']:
        print('val')
    elif strtgy[0:7] in ['ROE', 'ROA', 'GPA']:
        print('qua')
    elif strtgy[0:7] in ['1MRM', '3MRM', '6MRM', '12MRM']:
        print('mom')
    elif strtgy[0:7] in ['F_SCORE']:
        print(strtgy[-1])
    else:
        print('err')