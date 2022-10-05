from numpy import int32
import pandas as pd
from panel import Row

df_power = pd.read_excel(r"C:\Users\Administrator\Desktop\파이썬과제\FA-M3착공\시작DATA\Data\0902\POWER.xlsx")
df_macro = pd.read_excel(r"C:\Users\Administrator\Desktop\파이썬과제\FA-M3착공\기초Data\FAM3 평준화 착공 LIST_R7.xlsm")

#L1 = len(df_power.index)

filter_power = [df_power.drop['Sequence No'][i] for i in range(len(df_power.index)) if df_power['Sequence No'][i] in 'D']
# D로 시작하는 행 제거 -> 안됨 ㅠ
filter_macro = [df_macro.drop['평준화 GR NO'][i] for i in range(len(df_macro.index)) if df_macro['평준화 GR NO'][i] not in 'POWER']
# POWER만 검색
# 사용자가 착공확정수량 입력할 때 검사호기별, 재고 등 다 고려해서 적어야하는데 이걸 프로그램에서 검증해주는건가??


