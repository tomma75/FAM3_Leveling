from ast import Break
import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import math
import time
import numpy as np
import openpyxl as xl
from PyQt5 import QtCore, QtWidgets, QtGui
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QDoubleValidator, QStandardItemModel, QIcon, QStandardItem, QIntValidator, QFont
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QProgressBar, QPlainTextEdit, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QApplication, QCalendarWidget, QVBoxLayout, QFileDialog, QCheckBox
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QObject, QThread, QRect, QSize, QDate
import pandas as pd
import cx_Oracle
from collections import OrderedDict #ksm add
from collections import defaultdict #ksm add

import pandas as np
import numpy as np

# sample = pd.DataFrame({'id': ['A', 'A',  'C',  'D',  'E']
#                        , 'time_spend_company': [2, 1, 4, 5, 3]
#                        , 'satisfaction_level': [0.89,  0.86, 0.74, 0.67, 0.72]})

# sample = sample.sort_values(by=['id',
#                                 'time_spend_company',
#                                 'satisfaction_level'],
#                                 ascending=[True,
#                                             True,
#                                             False])

# print(sample)
# student_card = pd.DataFrame({'분류':['1','2','기타','폴더','파일명'],
#                              '분류별 상황':['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우','당일 착공분(or 긴급착공분)에 대해 검사설비 능력이 부족할 경우','MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우''output ➡ alarm','FAM3_AlarmList_20221028_시분초'],
#                              })
# print(student_card)



# =========================================================== st
def Alarm_all(df_sum,df_det,div,msc,smt,amo,ate,niz_a,niz_m,msg,ln,oq,sq,pt,nt,ecd):
            if str(div) == '1':
                df_sum = df_sum.append({
                    '분류' : str(div),
                    'MS CODE' : '-',
                    'SMT ASSY' : str(smt),
                    '수량' : int(amo),
                    '검사호기' : '-',
                    '부족 대수(특수,Power)' : 0,
                    '부족 시간(Main)' : 0,
                    'Message' : str(msg)
                    },ignore_index=True)
            elif str(div) == '2':
                df_sum = df_sum.append({
                    '분류' : str(div),
                    'MS CODE' : '-',
                    'SMT ASSY' : '-',
                    '수량' : '-',
                    '검사호기' : str(ate),
                    '부족 대수(특수,Power)' : int(niz_a),
                    '부족 시간(Main)' : 0,
                    'Message' : str(msg)
                    },ignore_index=True)
            elif str(div) == '기타':
                df_sum = df_sum.append({
                    '분류' : str(div),
                    'MS CODE' : str(msc),
                    'SMT ASSY' : '-',
                    '수량' : '-',
                    '검사호기' : '-',
                    '부족 대수(특수,Power)' : 0,
                    '부족 시간(Main)' : 0,
                    'Message' : str(msg)
                    },ignore_index=True)
            df_det = df_det.append({
                '분류':str(div),
                'L/N': str(ln), 
                'MS CODE' : str(msc), 
                'SMT ASSY' : str(smt), 
                '수주수량' : int(oq),
                '부족수량' : int(sq), 
                '검사호기' : str(ate), 
                '대상 검사시간(초)' : int(pt), 
                '필요시간(초)' : int(nt), 
                '완성예정일' : ecd
            },ignore_index=True)
            return(df_sum,df_det)

def Cycling(df_main,dict,col_name,col_name_W,t):#11/16
    df_Ate = df_main.sort_values(by=[col_name],ascending=True)
    df_Ate = df_Ate.drop_duplicates([col_name])
    df_Ate = df_Ate.reset_index(drop=True)
    for i in df_Ate.index:
        dict[df_Ate[col_name][i]] = float(i)
    df_main = df_main.sort_values(by=[col_name],ascending=False)
    df_main = df_main.reset_index(drop=True)
    for i in df_main.index:
        if df_main[col_name][i] == df_main[col_name][i+1]:
            if i == 0: 
                if dict[df_main[col_name][i]] == 0:
                    dict[df_main[col_name][i]] = df_Ate.shape[0] - t
                    df_main[col_name_W][i] = dict[df_main[col_name][i]] + 0.1
                elif dict[df_main[col_name][i]] == 1:
                    dict[df_main[col_name][i]] += t
                    df_main[col_name_W][i] = dict[df_main[col_name][i]] + 0.1
                else:
                    dict[df_main[col_name][i]] -= (t+1)
                    if dict[df_main[col_name][i]] < 0:
                        dict[df_main[col_name][i]] += df_Ate.shape[0]
                    df_main[col_name_W][i] = dict[df_main[col_name][i]] + 0.1
            else:
                dict[df_main[col_name][i]] += df_Ate.shape[0]
                df_main[col_name_W][i] = dict[df_main[col_name][i]] + 0.1
        else:
            break

    df_main = df_main.sort_values(by=[col_name_W],ascending=False)
    df_main = df_main.reset_index(drop=True)
    return(df_main)
        
#===================================================ksm add st 11/10 이미추가돼있음
df_levelingMain = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\input\Master_File\20220901\POWER.xlsx')
df_levelingMainDropSEQ = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No']=='Undep']
df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No']=='Uncor']
df_levelingMain = pd.concat([df_levelingMainDropSEQ, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
df_levelingMain['Linkage Number'] = df_levelingMain['Linkage Number'].astype(str)
df_levelingMain = df_levelingMain.reset_index(drop=True)

df_levelingMain.to_excel(r'C:\Users\Administrator\Desktop\테스트\POWER_미착공만.xlsx')
#====================================================add end 11/10

df_joinSmt = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow6.xlsx')
dict_smtCnt = {}
for i in df_joinSmt.index:
    dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]

df_addSmtAssy = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow10 - 복사본.xlsx')
df_addSmtAssyPower = df_addSmtAssy[df_addSmtAssy['PRODUCT_TYPE']=='POWER']
df_addSmtAssyPower = df_addSmtAssyPower.reset_index(drop=True)
df_addSmtAssyPower['SMT반영_착공량'] = 0
df_addSmtAssyPower['SMT반영_착공량_잔여'] = 0 #ADD 잔여적용 11/04
df_SMT_Alarm = pd.DataFrame(columns={'분류','MS CODE','SMT ASSY','수량','검사호기','부족 대수(특수,Power)','부족 시간(Main)','Message'},dtype=str)
df_SMT_Alarm['수량'] = df_SMT_Alarm['수량'] .astype(int)
df_SMT_Alarm['부족 시간(Main)'] =df_SMT_Alarm['부족 시간(Main)'].astype(int)
df_SMT_Alarm['부족 대수(특수,Power)'] =df_SMT_Alarm['부족 대수(특수,Power)'].astype(int)
df_SMT_Alarm = df_SMT_Alarm[['분류','MS CODE','SMT ASSY','수량','검사호기','부족 대수(특수,Power)','부족 시간(Main)','Message']]
df_Spcf_Alarm = pd.DataFrame(columns={'분류','L/N','MS CODE','SMT ASSY','수주수량','부족수량','검사호기','대상 검사시간(초)','필요시간(초)','완성예정일'},dtype=str)
df_Spcf_Alarm['수주수량'] = df_Spcf_Alarm['수주수량'] .astype(int)
df_Spcf_Alarm['부족수량'] =df_Spcf_Alarm['부족수량'].astype(int)
df_Spcf_Alarm['대상 검사시간(초)'] =df_Spcf_Alarm['대상 검사시간(초)'].astype(int)
df_Spcf_Alarm['필요시간(초)'] =df_Spcf_Alarm['필요시간(초)'].astype(int)
#df_Spcf_Alarm['완성예정일'] =df_Spcf_Alarm['완성예정일'].astype(datetime.datetime)
df_Spcf_Alarm = df_Spcf_Alarm[['분류','L/N','MS CODE','SMT ASSY','수주수량','부족수량','검사호기','대상 검사시간(초)','필요시간(초)','완성예정일']]
for i in df_addSmtAssyPower.index:
    if df_addSmtAssyPower['평준화_적용_착공량'][i] == 0:
        continue
    dict_smt_name = defaultdict(list) #리스트초기화
    dict_smt_name2 = defaultdict(list)
    t=0
    for j in range(1,6):
        if str(df_addSmtAssyPower[f'ROW{str(1)}'][i]) == '' and str(df_addSmtAssyPower[f'ROW{str(1)}'][i]) == 'nan':
            df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'기타',df_addSmtAssyPower['MSCODE'][i],df_addSmtAssyPower[f'ROW{str(j)}'][i],
            0,'-',0,0,'SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.',str(df_addSmtAssyPower['LINKAGE NO'][i]),
            df_addSmtAssyPower['평준화_적용_착공량'][i],0,0,0,df_addSmtAssyPower['완성\n지정일'][i])
        if str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != 'nan':
            if df_addSmtAssyPower[f'ROW{str(j)}'][i] in dict_smtCnt:
                dict_smt_name[df_addSmtAssyPower[f'ROW{str(j)}'][i]] = int(dict_smtCnt[df_addSmtAssyPower[f'ROW{str(j)}'][i]])
            else:
                dict_smt_name[df_addSmtAssyPower[f'ROW{str(j)}'][i]] = 0 #11/08
                # df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'기타',df_addSmtAssyPower['MSCODE'][i],df_addSmtAssyPower[f'ROW{str(j)}'][i],
                # 0,'-',0,0,'SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.',str(df_addSmtAssyPower['LINKAGE NO'][i]),
                # df_addSmtAssyPower['평준화_적용_착공량'][i],0,0,0,df_addSmtAssyPower['완성\n지정일'][i])
                # df_SMT_Alarm = df_SMT_Alarm.append({
                #                 '분류' : '기타',
                #                 'MS CODE' : df_addSmtAssyPower['MSCODE'][i],
                #                 'SMT ASSY' : df_addSmtAssyPower[f'ROW{str(j)}'][i],
                #                 '수량' : 0,
                #                 '검사호기' : '-',
                #                 '부족 대수(특수,Power)' : 0,
                #                 '부족 시간(Main)' : 0,
                #                 'Message' : 'SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'
                #                 },ignore_index=True)
                # df_Spcf_Alarm = df_Spcf_Alarm.append({
                #     '분류':'기타',
                #     'L/N': str(df_addSmtAssyPower['LINKAGE NO'][i]), 
                #     'MS CODE' : df_addSmtAssyPower['MSCODE'][i], 
                #     'SMT ASSY' : df_addSmtAssyPower[f'ROW{str(j)}'][i], 
                #     '수주수량' : df_addSmtAssyPower['평준화_적용_착공량'][i],
                #     '부족수량' : 0, 
                #     '검사호기' : '-', 
                #     '대상 검사시간(초)' : 0, 
                #     '필요시간(초)' : 0, 
                #     '완성예정일' : df_addSmtAssyPower['완성\n지정일'][i]
                # },ignore_index=True)
                t=1 #SMT 재고 없으면 긴급이 아닌경우에는 그냥 다음껄로 넘겨야한다. 
                break
        else:
            break
    dict_smt_name2 = OrderedDict(sorted(dict_smt_name.items(),key=lambda x : x[1],reverse=False))#한번에 처리하기위해 value값 내림차순으로 해서 딕셔너리 형태로 저장     
    if str(df_addSmtAssyPower['긴급오더'][i]) == '대상':
        for k in dict_smt_name2:
            dict_smtCnt[f'{k}'] -= df_addSmtAssyPower['평준화_적용_착공량'][i]
            if dict_smtCnt[f'{k}'] < 0:#여기까지(하고나면지우기)
                df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'1','-',k,dict_smtCnt[f'{k}'],'-',0,0,'[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%k,
                df_addSmtAssyPower['LINKAGE NO'][i],df_addSmtAssyPower['평준화_적용_착공량'][i],0-dict_smtCnt[f'{k}'],0,0,df_addSmtAssyPower['완성\n지정일'][i])
        df_addSmtAssyPower['SMT반영_착공량'][i] = df_addSmtAssyPower['평준화_적용_착공량'][i]
    else:
        if t==1 :  continue
        for k in dict_smt_name2:
            if dict_smt_name2[f'{k}'] > 0 :
                if dict_smt_name2[f'{next(iter(dict_smt_name2))}'] > df_addSmtAssyPower['평준화_적용_착공량'][i] : #사용하는 smt assy 들의 재고수량이 평준화 적용착공량보다 크면(생산여유재고있으면)
                    df_addSmtAssyPower['SMT반영_착공량'][i] = df_addSmtAssyPower['평준화_적용_착공량'][i] # 평준화 적용착공량으로 착공오더내림
                    dict_smtCnt[next(iter(dict_smt_name2))] -= df_addSmtAssyPower['평준화_적용_착공량'][i]
                else:
                    df_addSmtAssyPower['SMT반영_착공량'][i] = dict_smt_name2[f'{next(iter(dict_smt_name2))}']#딕셔너리 벨류값들 중 가장 작은 값으로 착공량 지정
                    dict_smtCnt[next(iter(dict_smt_name2))] -= dict_smt_name2[f'{next(iter(dict_smt_name2))}']
                    df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'1',df_addSmtAssyPower['MSCODE'][i],next(iter(dict_smt_name2)),df_addSmtAssyPower['평준화_적용_착공량'][i]-dict_smt_name2[f'{next(iter(dict_smt_name2))}'],
                    '-',0,0,'[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%next(iter(dict_smt_name2)),df_addSmtAssyPower['LINKAGE NO'][i],
                    df_addSmtAssyPower['평준화_적용_착공량'][i],df_addSmtAssyPower['평준화_적용_착공량'][i]-dict_smt_name2[f'{next(iter(dict_smt_name2))}'],
                    0,0,df_addSmtAssyPower['완성\n지정일'][i])
            else:
                df_addSmtAssyPower['SMT반영_착공량'][i] = 0 #재고없으면 0
                df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'1',df_addSmtAssyPower['MSCODE'][i],k,df_addSmtAssyPower['평준화_적용_착공량'][i]-dict_smt_name2[f'{k}'],
                '-',0,0,'[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%k,df_addSmtAssyPower['LINKAGE NO'][i],
                df_addSmtAssyPower['평준화_적용_착공량'][i],df_addSmtAssyPower['평준화_적용_착공량'][i]-dict_smt_name2[f'{k}'],
                0,0,df_addSmtAssyPower['완성\n지정일'][i])
for i in df_addSmtAssyPower.index:
    if df_addSmtAssyPower['잔여_착공량'][i] == 0:
        continue
    dict_smt_name = defaultdict(list) #리스트초기화
    dict_smt_name2 = defaultdict(list)
    t=0
    for j in range(1,6):
        if str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != 'nan':
            if df_addSmtAssyPower[f'ROW{str(j)}'][i] in dict_smtCnt:
                dict_smt_name[df_addSmtAssyPower[f'ROW{str(j)}'][i]] = int(dict_smtCnt[df_addSmtAssyPower[f'ROW{str(j)}'][i]])
            else:
                df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'기타',df_addSmtAssyPower['MSCODE'][i],df_addSmtAssyPower[f'ROW{str(j)}'][i],
                0,'-',0,0,'SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.',str(df_addSmtAssyPower['LINKAGE NO'][i]),
                df_addSmtAssyPower['잔여_착공량'][i],0,0,0,df_addSmtAssyPower['완성\n지정일'][i])
                t = 1
                break
        else:
            break
    dict_smt_name2 = OrderedDict(sorted(dict_smt_name.items(),key=lambda x : x[1],reverse=False))#한번에 처리하기위해 value값 내림차순으로 해서 딕셔너리 형태로 저장       
    if t==1 :  
        t = 0
        continue
    if dict_smt_name2[f'{next(iter(dict_smt_name2))}'] > 0 :
        if dict_smt_name2[f'{next(iter(dict_smt_name2))}'] > df_addSmtAssyPower['잔여_착공량'][i] : 
            df_addSmtAssyPower['SMT반영_착공량_잔여'][i] = df_addSmtAssyPower['잔여_착공량'][i]
            dict_smtCnt[next(iter(dict_smt_name2))] -= df_addSmtAssyPower['잔여_착공량'][i]
        else:
            df_addSmtAssyPower['SMT반영_착공량_잔여'][i] = dict_smt_name2[f'{next(iter(dict_smt_name2))}']
            dict_smtCnt[next(iter(dict_smt_name2))] -= dict_smt_name2[f'{next(iter(dict_smt_name2))}']
            df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'1',df_addSmtAssyPower['MSCODE'][i],next(iter(dict_smt_name2)),df_addSmtAssyPower['잔여_착공량'][i]-dict_smt_name2[f'{next(iter(dict_smt_name2))}'],
            '-',0,0,'[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%next(iter(dict_smt_name2)),df_addSmtAssyPower['LINKAGE NO'][i],
            df_addSmtAssyPower['잔여_착공량'][i],df_addSmtAssyPower['잔여_착공량'][i]-dict_smt_name2[f'{next(iter(dict_smt_name2))}'],
            0,0,df_addSmtAssyPower['완성\n지정일'][i])
    else:
        df_addSmtAssyPower['잔여_착공량'][i] = 0 #재고없으면 0

df_SMT_Alarm = df_SMT_Alarm.drop_duplicates(subset=['SMT ASSY','분류','MS CODE'])
df_Spcf_Alarm = df_Spcf_Alarm.drop_duplicates(subset=['SMT ASSY','수주수량','L/N','MS CODE'])
df_addSmtAssyPower['LINKAGE NO'] = df_addSmtAssyPower['LINKAGE NO'].astype(str)
df_addSmtAssyPower['Linkage Number'] = df_addSmtAssyPower['Linkage Number'].astype(str)
df_SMT_Alarm.to_excel(r'C:\Users\Administrator\Desktop\테스트\SMT알람저장1.xlsx')
df_Spcf_Alarm.to_excel(r'C:\Users\Administrator\Desktop\테스트\상세알람저장1.xlsx')
df_addSmtAssyPower.to_excel(r'C:\Users\Administrator\Desktop\테스트\SMT나눈거.xlsx')# SMT 재고고려 착공량 완료, 설비능력, 잔여착공량고려필요함

df_PowerATE = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\input\Master_File_Power\FAM3 전원 LINE 생산 조건.xlsx')
dict_MODEL_TE = defaultdict(list)
dict_MODEL_Ra = defaultdict(list)
dict_MODEL_Ate = defaultdict(list)
dict_cycling_cnt = defaultdict(list) #add 11/11 사이클링
df_addSmtAssyPower['설비능력반영합'] = 0
df_addSmtAssyPower['설비능력반영_착공공수'] = 0
df_addSmtAssyPower['설비능력반영_착공공수_잔여'] = 0 #add 11/04 잔여
df_addSmtAssyPower['설비능력반영_착공량'] = 0
powerOrderCnt_copy = 200 #공수설정
dict_Power_less_add = defaultdict(list)
for i in df_PowerATE.index:
    dict_MODEL_TE[df_PowerATE['MODEL'][i]] = float(df_PowerATE['공수'][i])
    if str(df_PowerATE['최대허용비율'][i]) == '' or str(df_PowerATE['최대허용비율'][i]) =='nan':
        df_PowerATE['최대허용비율'][i] = df_PowerATE['최대허용비율'][i-1]
    dict_MODEL_Ra[df_PowerATE['MODEL'][i]] = round(float(df_PowerATE['최대허용비율'][i])*powerOrderCnt_copy)
    dict_MODEL_Ate[df_PowerATE['MODEL'][i]] = df_PowerATE['검사호기'][i]
    dict_Power_less_add[df_PowerATE['MODEL'][i]] = 0
#설비능력고려 11/07
t=0
for i in df_addSmtAssyPower.index:
    if float(df_addSmtAssyPower['SMT반영_착공량'][i]) == float(0) : 
                continue
    if str(df_addSmtAssyPower['MSCODE'][i])[:4] in dict_MODEL_TE.keys():
        if str(df_addSmtAssyPower['긴급오더'][i]) == '대상':
            df_addSmtAssyPower['설비능력반영_착공공수'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
            df_addSmtAssyPower['설비능력반영_착공량'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]
            powerOrderCnt_copy -= df_addSmtAssyPower['SMT반영_착공량'][i]*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
            dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] -= float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
            if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] < float(0) : t = 1
            if powerOrderCnt_copy < 0 : 
                t = 2
                if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] < float(0) : t = 1
            if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] < float(0) or powerOrderCnt_copy < 0:
                if t == 1:
                    dict_Power_less_add[str(df_addSmtAssyPower['MSCODE'][i])[:4]] += -dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                    t=0
                elif t == 2:
                    dict_Power_less_add[str(df_addSmtAssyPower['MSCODE'][i])[:4]] += -powerOrderCnt_copy
                    t=0
                df_SMT_Alarm,df_Spcf_Alarm =Alarm_all(df_SMT_Alarm,df_Spcf_Alarm,'2',df_addSmtAssyPower['MSCODE'][i],'-',0,dict_MODEL_Ate[str(df_addSmtAssyPower['MSCODE'][i])[:4]],
                dict_Power_less_add[str(df_addSmtAssyPower['MSCODE'][i])[:4]],0,'검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.',
                df_addSmtAssyPower['LINKAGE NO'][i],df_addSmtAssyPower['SMT반영_착공량'][i],dict_Power_less_add[str(df_addSmtAssyPower['MSCODE'][i])[:4]],
                0,0,df_addSmtAssyPower['완성\n지정일'][i])
        else:
            if float(df_addSmtAssyPower['SMT반영_착공량'][i]) == float(0) : 
                continue
            if powerOrderCnt_copy > float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
                if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] > float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
                    df_addSmtAssyPower['설비능력반영_착공공수'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                    df_addSmtAssyPower['설비능력반영_착공량'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]
                    dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] -= float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                    powerOrderCnt_copy -= float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                else:
                    df_addSmtAssyPower['설비능력반영_착공공수'][i] = dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                    df_addSmtAssyPower['설비능력반영_착공량'][i] = math.ceil(dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] / dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]])
                    powerOrderCnt_copy -= df_addSmtAssyPower['설비능력반영_착공공수'][i]*dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                    dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] = 0
            elif powerOrderCnt_copy == 0 or powerOrderCnt_copy <0 :
                break
            else:
                df_addSmtAssyPower['설비능력반영_착공공수'][i] = powerOrderCnt_copy
                df_addSmtAssyPower['설비능력반영_착공량'][i] = math.ceil(powerOrderCnt_copy / dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]])
                powerOrderCnt_copy = 0
                break
    else:
        continue

for i in df_addSmtAssyPower.index: #add 11/04 잔여
    if float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i]) == float(0) : 
        continue
    if str(df_addSmtAssyPower['MSCODE'][i])[:4] in dict_MODEL_TE.keys():
        if float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i]) == float(0) : 
            continue
        if powerOrderCnt_copy > float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
            if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] > float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
                df_addSmtAssyPower['설비능력반영_착공공수_잔여'][i] = df_addSmtAssyPower['SMT반영_착공량_잔여'][i]*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                df_addSmtAssyPower['설비능력반영_착공량'][i] += df_addSmtAssyPower['SMT반영_착공량_잔여'][i]
                dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] -= float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                powerOrderCnt_copy -= float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
            else:
                df_addSmtAssyPower['설비능력반영_착공공수_잔여'][i] = dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] #/ dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                df_addSmtAssyPower['설비능력반영_착공량'][i] += math.ceil(dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] / dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]])
                powerOrderCnt_copy -= dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]]
                dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] = 0
        elif powerOrderCnt_copy == 0 or powerOrderCnt_copy <0 :
            break
        elif powerOrderCnt_copy < float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
            if dict_MODEL_Ra[str(df_addSmtAssyPower['MSCODE'][i])[:4]] > float(df_addSmtAssyPower['SMT반영_착공량_잔여'][i])*dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]]:
                df_addSmtAssyPower['설비능력반영_착공공수_잔여'][i] = powerOrderCnt_copy
                df_addSmtAssyPower['설비능력반영_착공량'][i] += math.ceil(powerOrderCnt_copy / dict_MODEL_TE[str(df_addSmtAssyPower['MSCODE'][i])[:4]])
                powerOrderCnt_copy = 0
            else:
                continue
        else:
            continue
    else:
        continue
for i in df_addSmtAssyPower.index:
    df_addSmtAssyPower['설비능력반영합'][i] = df_addSmtAssyPower['설비능력반영_착공공수'][i] + df_addSmtAssyPower['설비능력반영_착공공수_잔여'][i]
    dict_cycling_cnt[df_addSmtAssyPower['Linkage Number'][i]] = df_addSmtAssyPower['설비능력반영_착공량'][i] #사이클링 11/11

zero = df_addSmtAssyPower[df_addSmtAssyPower['설비능력반영합']==0].index
df_addSmtAssyPower.drop(zero, inplace=True)
df_addSmtAssyPower = df_addSmtAssyPower.drop(['설비능력반영합'],axis='columns')
df_addSmtAssyPower['LINKAGE NO'] = df_addSmtAssyPower['LINKAGE NO'].astype(str)
df_addSmtAssyPower['Linkage Number'] = df_addSmtAssyPower['Linkage Number'].astype(str)

df_SMT_Alarm = df_SMT_Alarm.drop_duplicates(subset=['검사호기','분류','Message','MS CODE','SMT ASSY'],keep='last')
df_Spcf_Alarm = df_Spcf_Alarm.drop_duplicates(subset=['분류','L/N','MS CODE','완성예정일'],keep='last')
df_addSmtAssyPower = df_addSmtAssyPower.reset_index(drop=True)
df_SMT_Alarm = df_SMT_Alarm.sort_values(by=['분류',
                                            '수량'],
                                            ascending=[True,
                                                        True])
df_Spcf_Alarm = df_Spcf_Alarm.sort_values(by=['분류',
                                                '완성예정일',
                                                'MS CODE',
                                                'SMT ASSY'],
                                                ascending=[True,
                                                            True,
                                                            True,
                                                            True])
df_SMT_Alarm = df_SMT_Alarm.reset_index(drop=True)
df_SMT_Alarm.index = df_SMT_Alarm.index+1
df_Spcf_Alarm = df_Spcf_Alarm.reset_index(drop=True)
df_Spcf_Alarm.index = df_Spcf_Alarm.index+1
df_explain = pd.DataFrame({'분류': ['1','2','기타1','폴더','파일명'] ,
                            '분류별 상황' : ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                            '당일 착공분(or 긴급착공분)에 대해 검사설비 능력이 부족할 경우',
                                            'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                            'output ➡ alarm',
                                            'FAM3_AlarmList_20221028_시분초']})
Alarmdate = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
PowerAlarm_path = r'.\\input\\AlarmList_Power\\FAM3_AlarmList_' + Alarmdate + r'.xlsx'
writer = pd.ExcelWriter(PowerAlarm_path,engine='xlsxwriter')
df_SMT_Alarm.to_excel(writer,sheet_name='정리')
df_Spcf_Alarm.to_excel(writer,sheet_name='상세')
df_explain.to_excel(writer,sheet_name='설명')
writer.save()

df_addSmtAssyPower.to_excel(r'C:\Users\Administrator\Desktop\테스트\설비.xlsx')
#ksm add st 11/10 사이클링

df_levelingPower = pd.merge(df_addSmtAssyPower,df_levelingMain,left_on='Linkage Number',right_on='Linkage Number',how='right')
df_levelingPower = df_levelingPower.dropna(subset=['설비능력반영_착공량'])
df_levelingPower = df_levelingPower.sort_values(by=['MS-CODE',
                                                'Scheduled End Date',],
                                                ascending=[True,
                                                            True])
# df_levelingPower = df_levelingPower.rename(columns={'Linkage Number_y' : 'Linkage Number'})
df_levelingPower.to_excel(r'C:\Users\Administrator\Desktop\테스트\1번.xlsx')
df_Cycling = df_levelingPower
df_Cycling = df_Cycling.dropna()
for i in df_levelingPower.index:
    add = df_levelingPower.loc[i]
    if dict_cycling_cnt[df_levelingPower['Linkage Number'][i]] > 0:
        df_Cycling = df_Cycling.append(add,ignore_index = True)
        dict_cycling_cnt[df_levelingPower['Linkage Number'][i]] -= 1
    else:
        continue

## KSM 사이클링 추가 ST ##
dict_Ate = defaultdict(list)
df_Cycling['Cycling'] = ''
df_Ate = df_Cycling.sort_values(by=['대표모델'],ascending=True)
df_Ate = df_Ate.drop_duplicates(['대표모델'])
df_Ate = df_Ate.reset_index(drop=True)
for i in df_Ate.index:
    dict_Ate[df_Ate['대표모델'][i]] = float(i)
for i in df_Cycling.index:
    if i == 0 :
        df_Cycling['Cycling'][i] = dict_Ate[df_Cycling['대표모델'][i]]
        dict_Ate[df_Cycling['대표모델'][i]] += df_Ate.shape[0]
    elif df_Cycling['대표모델'][i] == df_Cycling['대표모델'][i-1]:
        df_Cycling['Cycling'][i] = dict_Ate[df_Cycling['대표모델'][i]]
        dict_Ate[df_Cycling['대표모델'][i]] += df_Ate.shape[0]
    else:
        df_Cycling['Cycling'][i] = dict_Ate[df_Cycling['대표모델'][i]]
        dict_Ate[df_Cycling['대표모델'][i]] += df_Ate.shape[0]
df_Cycling = df_Cycling.sort_values(by=['Cycling'],ascending=False)
df_Cycling = df_Cycling.reset_index(drop=True)

for i in df_Cycling.index:
    df_Cycling = Cycling(df_Cycling,dict_Ate,'대표모델','Cycling',i)
    if df_Cycling['Cycling'][i] != df_Cycling['Cycling'][i+1]:
        break

df_Cycling.to_excel(r'C:\Users\Administrator\Desktop\테스트\3번.xlsx')
df_Cycling = df_Cycling.sort_values(by=['Cycling'],ascending=True)

## KSM 사이클링 추가 END ##
df_Cycling = df_Cycling.reset_index(drop=True)
df_Cycling['No (*)'] = (df_Cycling.index.astype(int) + 1) * 10
df_Cycling['Scheduled Start Date (*)'] = 'test' #self.labelDate.text()
df_Cycling['Planned Order'] = df_Cycling['Planned Order'].astype(int).astype(str).str.zfill(10)
df_Cycling['Scheduled End Date'] = df_Cycling['Scheduled End Date'].astype(str).str.zfill(10)
df_Cycling['Specified Start Date'] = df_Cycling['Specified Start Date'].astype(str).str.zfill(10)
df_Cycling['Specified End Date'] = df_Cycling['Specified End Date'].astype(str).str.zfill(10)
df_Cycling['Spec Freeze Date'] = df_Cycling['Spec Freeze Date'].astype(str).str.zfill(10)
df_Cycling['Component Number'] = df_Cycling['Component Number'].astype(int).astype(str).str.zfill(4)
dfMergeOrderResult = df_Cycling[['No (*)', 
                                                                'Sequence No', 
                                                                'Production Order', 
                                                                'Planned Order', 
                                                                'Manual', 
                                                                'Scheduled Start Date (*)', 
                                                                'Scheduled End Date', 
                                                                'Specified Start Date', 
                                                                'Specified End Date', 
                                                                'Demand destination country', 
                                                                'MS-CODE', 
                                                                'Allocate', 
                                                                'Spec Freeze Date', 
                                                                'Linkage Number', 
                                                                'Order Number', 
                                                                'Order Item', 
                                                                'Combination flag', 
                                                                'Project Definition', 
                                                                'Error message', 
                                                                'Leveling Group', 
                                                                'Leveling Class', 
                                                                'Planning Plant', 
                                                                'Component Number', 
                                                                'Serial Number']]
dfMergeOrderResult = dfMergeOrderResult.reset_index(drop=True)
dfMergeOrderResult.to_excel(r'C:\Users\Administrator\Desktop\테스트\머지.xlsx',index=False)

# ===========================================================end

# df_joinSmt = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow6.xlsx')
# dict_smtCnt1 = {}
# for i in df_joinSmt.index:
#     dict_smtCnt1[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
# dict_smtCnt = dict_smtCnt1
# df_addSmtAssyPower = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow10.xlsx')
# df_addSmtAssyPower['SMT반영_착공량'] = 0
# df_SMT_Alarm = pd.DataFrame(columns={'분류','MS CODE','SMT ASSY','수량','검사호기','부족 시간','Message'})
# df_SMT_Alarm['수량'] = df_SMT_Alarm['수량'] .astype(int)
# df_SMT_Alarm['부족 시간'] =df_SMT_Alarm['부족 시간'].astype(int)
# df_SMT_Alarm = df_SMT_Alarm[['분류','MS CODE','SMT ASSY','수량','검사호기','부족 시간','Message']]
# df_Spcf_Alarm = pd.DataFrame({'분류','L/N','MS CODE','SMT ASSY','수주수량','부족수량','검사호기','대상 검사시간(초)','필요시간(초)','완성예정일'})
# df_Spcf_Alarm['완성예정일'] =df_Spcf_Alarm['완성예정일'].astype(datetime)
# for i in df_addSmtAssyPower.index:
#     if df_addSmtAssyPower['PRODUCT_TYPE'][i] == 'POWER':
#         dict_smt_name = defaultdict(list) #리스트초기화
#         dict_smt_name2 = defaultdict(list)
#         t=0
#         for j in range(1,6):
#             if str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssyPower[f'ROW{str(j)}'][i]) != 'nan':
#                 if df_addSmtAssyPower[f'ROW{str(j)}'][i] in dict_smtCnt:
#                     dict_smt_name[df_addSmtAssyPower[f'ROW{str(j)}'][i]] = int(dict_smtCnt[df_addSmtAssyPower[f'ROW{str(j)}'][i]])
#                 else:
#                     # logging.warning('「사양 : %s」의 SmtAssy가 %s 파일에 등록되지 않았습니다. 등록 후, 다시 실행해주세요.',
#                     #                 df_addSmtAssyPower['MS Code'][i],
#                     #                 'list_masterFile[6]')
#                     t=1 #SMT 재고 없으면 긴급이 아닌경우에는 그냥 다음껄로 넘겨야한다.
#                     break
#             else:
#                 break

#         dict_smt_name2 = OrderedDict(sorted(dict_smt_name.items(),key=lambda x : x[1],reverse=False))#한번에 처리하기위해 value값 내림차순으로 해서 딕셔너리 형태로 저장       
#         if df_addSmtAssyPower['긴급오더'][i] == '대상':
#             for k in dict_smt_name2:
#                 dict_smtCnt[f'{k}'] -= df_addSmtAssyPower['평준화_적용_착공량'][i]

#                 if dict_smtCnt[f'{k}'] < 0:
#                     df_add_AlarmData={
#                                     '분류' : '1','MS CODE' : '-','SMT ASSY' : k,'수량' : 0-dict_smtCnt[f'{k}'],'검사호기' : '-','부족 시간' : '-',
#                                     'Message' : '[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%k
#                                     }
#                     df_SMT_Alarm = df_SMT_Alarm.append(df_add_AlarmData,ignore_index=True)
#                     df_Spcf_Alarm ={
#                         '완성예정일' : datetime.datetime.now()
#                     }

#             df_addSmtAssyPower['SMT반영_착공량'][i] = df_addSmtAssyPower['평준화_적용_착공량'][i]
#         else:
#             if t==1 :  continue
#             if dict_smt_name2[f'{next(iter(dict_smt_name2))}'] != 0 :
#                 if dict_smt_name2[f'{next(iter(dict_smt_name2))}'] > df_addSmtAssyPower['평준화_적용_착공량'][i] : #사용하는 smt assy 들의 재고수량이 평준화 적용착공량보다 크면(생산여유재고있으면)
#                     df_addSmtAssyPower['SMT반영_착공량'][i] = df_addSmtAssyPower['평준화_적용_착공량'][i] # 평준화 적용착공량으로 착공오더내림
#                 else:
#                     df_addSmtAssyPower['SMT반영_착공량'][i] = dict_smt_name2[f'{next(iter(dict_smt_name2))}']#딕셔너리 벨류값들 중 가장 작은 값으로 착공량 지정
#             else:
#                 df_addSmtAssyPower['SMT반영_착공량'][i] = 0 #재고없으면 0
# df_SMT_Alarm = df_SMT_Alarm.drop_duplicates(subset=['SMT ASSY','수량'])
# df_SMT_Alarm.index = df_SMT_Alarm.index+1
# df_SMT_Alarm.to_excel('.\\debug\\알람테스트.xlsx')

# df_SMT_Alarm = pd.DataFrame(columns={'분류','MS CODE','SMT ASSY','수량','검사호기','부족 시간','Message'},dtype=str)
# df_SMT_Alarm['수량'] = df_SMT_Alarm['수량'] .astype(int)
# df_SMT_Alarm['부족 시간'] =df_SMT_Alarm['부족 시간'].astype(int)
# k = 'test-ttt'
# y = 5
# df_SMT_Alarm2={
#                                                     '분류' : '1','MS CODE' : '-','SMT ASSY' : k,'수량' : y,'검사호기' : '-','부족 시간' : '-',
#                                                     'Message' : '[SMT ASSY : %s]가 부족합니다. SMT ASSY 제작을 지시해주세요.'%k
#                                                     }
# df_SMT_Alarm = df_SMT_Alarm.append(df_SMT_Alarm2,ignore_index=True)
# df_SMT_Alarm.to_excel(r'C:\Users\Administrator\Desktop\테스트저장폴더')


# df_PowerATE = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\input\DB\FAM3 전원 LINE 생산 조건.xlsx',header=2)
# df_addSmtAssyPower = pd.read_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\test2-복사본.xlsx')
# dict_Ate_T = defaultdict(list)
# dict_AteA = defaultdict(list)
# df_addSmtAssyPower['설비능력반영_착공량'] = 0
# k =0
# X = 200
# for i in df_PowerATE.index:
#     dict_Ate_T[df_PowerATE['MODEL'][i]] = float(df_PowerATE['공수'][i])
#     if str(df_PowerATE['최대허용비율'][i]) == '' or str(df_PowerATE['최대허용비율'][i]) =='nan':
#         df_PowerATE['최대허용비율'][i] = df_PowerATE['최대허용비율'][i-1]
#     dict_AteA[df_PowerATE['MODEL'][i]] = float(df_PowerATE['최대허용비율'][i])*X
#     k += 1
# for i in df_addSmtAssyPower.index:
#     for j in df_PowerATE.index:
#         if str(df_PowerATE['MODEL'][j]) in str(df_addSmtAssyPower['MSCODE'][i]):
#             if dict_AteA[df_PowerATE['MODEL'][j]] > 0:
#                 if float(df_addSmtAssyPower['SMT반영_착공량'][i]) == 0 : continue
#                 if X > float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_Ate_T[df_PowerATE['MODEL'][j]]:
#                     if dict_AteA[df_PowerATE['MODEL'][j]] > float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_Ate_T[df_PowerATE['MODEL'][j]]:
#                         df_addSmtAssyPower['설비능력반영_착공량'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]
#                         dict_AteA[df_PowerATE['MODEL'][j]] -= float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_Ate_T[df_PowerATE['MODEL'][j]]
#                         X -= float(df_addSmtAssyPower['SMT반영_착공량'][i])*dict_Ate_T[df_PowerATE['MODEL'][j]]
#                     else:
#                         df_addSmtAssyPower['설비능력반영_착공량'][i] = dict_AteA[df_PowerATE['MODEL'][j]] / dict_Ate_T[df_PowerATE['MODEL'][j]]
#                         X -= dict_AteA[df_PowerATE['MODEL'][j]]
#                         dict_AteA[df_PowerATE['MODEL'][j]] = 0
#                 else:
#                     df_addSmtAssyPower['설비능력반영_착공량'][i] = X / dict_Ate_T[df_PowerATE['MODEL'][j]]
#                     X = 0
                    
#         else:continue

# #DB에 모델별 공수 추가
# for i in df_PowerATE.index: 
#     dict_Ate_T[df_PowerATE['MODEL'][i]] = df_PowerATE['공수'][i]
#     k +=1
# X=int(200)
# D=df_PowerATE['최대허용비율'][0]
# A=int(X*D)
# B=int(X*D)
# print(df_PowerATE['MODEL'][1])
# print(str(df_addSmtAssyPower['MSCODE'][1]))
# for i in df_addSmtAssyPower.index:
#     if X==0 : Break
#     if df_PowerATE['MODEL'][0] in str(df_addSmtAssyPower['MSCODE'][i]) :
#         if X>0 and A > 0:
#             if A > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]] and X > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]]:
#                 print(df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]])
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]
#                 A -= df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]]
#                 X -= df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]]
#             elif X > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]] and A < df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]]:
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = A // B
#                 X -= A
#                 A = 0
#             elif X < df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][0]]:
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = X // dict_Ate_T[df_PowerATE['MODEL'][0]]
#                 Break
#         else:
#             continue
#     elif df_PowerATE['MODEL'][1] in str(df_addSmtAssyPower['MSCODE'][i]):
#         if X>0 and B > 0:
#             if B > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]] and X > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]]:
#                 print(df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]])
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = df_addSmtAssyPower['SMT반영_착공량'][i]
#                 B -= df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]]
#                 X -= df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]]
#             elif X > df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]] and A < df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]]:
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = A // B
#                 X -= B
#                 B = 0
#             elif X < df_addSmtAssyPower['SMT반영_착공량'][i] * dict_Ate_T[df_PowerATE['MODEL'][1]]:
#                 df_addSmtAssyPower['설비능력반영_착공량'][i] = X // dict_Ate_T[df_PowerATE['MODEL'][1]]
#                 Break
#         else:
#             continue
# df_addSmtAssyPower.to_excel(r'C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\atetest.xlsx')

