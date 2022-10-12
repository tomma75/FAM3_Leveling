from numpy import int32
import pandas as pd
from panel import Row

df_power = pd.read_excel(r"C:\Users\Administrator\Desktop\파이썬과제\FA-M3착공\시작DATA\Data\0902\POWER.xlsx")



df_FP = [df_power.drop['Sequence No'][i] for i in range(len(df_power.index)) if df_power['Sequence No'][i] in 'D']
# D로 시작하는 행 제거 -> 안됨 ㅠ
df_FM = [df_macro.drop['평준화 GR NO'][i] for i in range(len(df_macro.index)) if df_macro['평준화 GR NO'][i] not in 'POWER']
# POWER만 검색
# 사용자가 착공확정수량 입력할 때 검사호기별, 재고 등 다 고려해서 적어야하는데 이걸 프로그램에서 검증해주는건가??
df_FP.join(df_FM.set_index('Linkage Number')['착공확정수량'],on='Linkage Number')
# 필터링된 파워 데이터프레임에 필터링된 매크로엑셀파일의 확정수량을 Vlookup으로 설정해줌
df_final = [df_power.drop['착공확정수량'][i] for i in range(len(df_power.index)) if df_power['착공확정수량'][i] in '']
# 착공확정수량이 Nan값이면 행 제거
df_final['착공순번'] = [df_final['착공순번'][i] == j for i in range(df_final.index) for j in ((df_final.index)/5)]
# 착공순번 column 생성 후 순번 지정
df_final.sort_values('착공순번',ascending=True)
# 착공순번 오름차순 설정
df_final['착공순번'] = []


#L1 = len(df_power.index)
CG_DF_max = 100 # 착공확정수량 합
CG_DF_ALam = 미착공수주잔 총합(열 더하는거) >[today() - df_power.sort_values('완성예정일',ascending=False)[0]] * CG_DF_max
#day 뺀 값을 int형식으로 가져와야함
for i in range(df_power.index):
    while CG_DF_sum < CG_DF_max:
        if df_power['미착공수주잔'][i] < CG_DF_max*0.5:
            CG_DF_sum = df_power['미착공수주잔'][i] + CG_DF_sum
        else:
            df_power['완성예정일'][i]