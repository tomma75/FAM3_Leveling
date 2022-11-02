<<<<<<< HEAD
from codecs import ignore_errors
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


df_addSmtAssy = pd.read_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow9.xlsx")
df_PowerSelect = df_addSmtAssy[df_addSmtAssy['PRODUCT_TYPE'] == 'POWER'].reset_index(drop=True)
df_POWER = pd.read_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\input\Master_File\20221006\POWER.xlsx")
#POWER만 선택
A_MAX = 200  #설정 최대생산대수
B_MAX = 200  #설정 최대생산대수
df_ATE_A = []
add = []
df_ATE_B = []
CNT_ATE_A = 0 
CNT_ATE_B = 0 
##################TEST######################
df_ATE_A = df_PowerSelect.loc[0:1]
df_ATE_A = df_ATE_A.drop(1,axis=0)
df_ATE_B = df_PowerSelect.loc[0:1]
df_ATE_B = df_ATE_B.drop(1,axis=0)

############################################
for i in range(len(df_PowerSelect.index)):
    if df_PowerSelect['ATE_NO'][i] == 'A':
        if A_MAX < CNT_ATE_A : continue    
        CNT_ATE_A += df_PowerSelect['미착공수주잔'][i]  #최대수량까지 ADD
        if CNT_ATE_A < A_MAX :
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_A = pd.merge(df_ATE_A,add,how='outer')
            Save_BFCNT_A = CNT_ATE_A
        else:
            df_PowerSelect['미착공수주잔'][i] = A_MAX - Save_BFCNT_A
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_A = pd.merge(df_ATE_A,add,how='outer')
            df_ATE_A = df_ATE_A.astype({'Linkage Number':'str'})
            # 어차피 착공내리면 다음날 새로 긁어서 하기때문에 수주잔 값 변경안해주어도 괜찮음
            if CNT_ATE_A >A_MAX and CNT_ATE_B > B_MAX : break
    elif df_PowerSelect['ATE_NO'][i] =='B':
        if B_MAX < CNT_ATE_B : continue    
        CNT_ATE_B += df_PowerSelect['미착공수주잔'][i]
        if CNT_ATE_B < B_MAX :
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_B = pd.merge(df_ATE_B,add,how='outer')
            Save_BFCNT_B = CNT_ATE_B
        else:
            df_PowerSelect['미착공수주잔'][i] = B_MAX - Save_BFCNT_B
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_B = pd.merge(df_ATE_B,add,how='outer')
            df_ATE_B = df_ATE_B.astype({'Linkage Number':'str'})
            # 어차피 착공내리면 다음날 새로 긁어서 하기때문에 수주잔 값 변경안해주어도 괜찮음
            if CNT_ATE_A >A_MAX and CNT_ATE_B > B_MAX : break
    else:
        #QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
        continue
df_ATE_A = df_ATE_A[df_ATE_A['ATE_NO'] == 'A'].reset_index(drop=True)
df_ATE_B = df_ATE_B[df_ATE_B['ATE_NO'] == 'B'].reset_index(drop=True)
df_TH_ATE = pd.merge(df_ATE_A,df_ATE_B,how='outer').reset_index(drop=True)
df_TH_ATE = df_TH_ATE[['Linkage Number','미착공수주잔']]
# Max 수량만큼 합치기
df_POWER['미착공수주잔']=0
df_POWER = df_POWER.astype({'Linkage Number':'str','No (*)':'str','Production Order':'str','Planned Order':'str'})
df_POWER = pd.merge(df_TH_ATE,how='left')


df_POWER.to_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\ksmtest\test_TH.xlsx",index=False)
=======
from codecs import ignore_errors
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


df_addSmtAssy = pd.read_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\Debug\flow9.xlsx")
df_PowerSelect = df_addSmtAssy[df_addSmtAssy['PRODUCT_TYPE'] == 'POWER'].reset_index(drop=True)
df_POWER = pd.read_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\input\Master_File\20221006\POWER.xlsx")
#POWER만 선택
A_MAX = 200  #설정 최대생산대수
B_MAX = 200  #설정 최대생산대수
df_ATE_A = []
add = []
df_ATE_B = []
CNT_ATE_A = 0 
CNT_ATE_B = 0 
##################TEST######################
df_ATE_A = df_PowerSelect.loc[0:1]
df_ATE_A = df_ATE_A.drop(1,axis=0)
df_ATE_B = df_PowerSelect.loc[0:1]
df_ATE_B = df_ATE_B.drop(1,axis=0)

############################################
for i in range(len(df_PowerSelect.index)):
    if df_PowerSelect['ATE_NO'][i] == 'A':
        if A_MAX < CNT_ATE_A : continue    
        CNT_ATE_A += df_PowerSelect['미착공수주잔'][i]  #최대수량까지 ADD
        if CNT_ATE_A < A_MAX :
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_A = pd.merge(df_ATE_A,add,how='outer')
            Save_BFCNT_A = CNT_ATE_A
        else:
            df_PowerSelect['미착공수주잔'][i] = A_MAX - Save_BFCNT_A
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_A = pd.merge(df_ATE_A,add,how='outer')
            df_ATE_A = df_ATE_A.astype({'Linkage Number':'str'})
            # 어차피 착공내리면 다음날 새로 긁어서 하기때문에 수주잔 값 변경안해주어도 괜찮음
            if CNT_ATE_A >A_MAX and CNT_ATE_B > B_MAX : break
    elif df_PowerSelect['ATE_NO'][i] =='B':
        if B_MAX < CNT_ATE_B : continue    
        CNT_ATE_B += df_PowerSelect['미착공수주잔'][i]
        if CNT_ATE_B < B_MAX :
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_B = pd.merge(df_ATE_B,add,how='outer')
            Save_BFCNT_B = CNT_ATE_B
        else:
            df_PowerSelect['미착공수주잔'][i] = B_MAX - Save_BFCNT_B
            add = df_PowerSelect.loc[i:i+1]
            add = add.drop(i+1,axis=0)
            df_ATE_B = pd.merge(df_ATE_B,add,how='outer')
            df_ATE_B = df_ATE_B.astype({'Linkage Number':'str'})
            # 어차피 착공내리면 다음날 새로 긁어서 하기때문에 수주잔 값 변경안해주어도 괜찮음
            if CNT_ATE_A >A_MAX and CNT_ATE_B > B_MAX : break
    else:
        #QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
        continue
df_ATE_A = df_ATE_A[df_ATE_A['ATE_NO'] == 'A'].reset_index(drop=True)
df_ATE_B = df_ATE_B[df_ATE_B['ATE_NO'] == 'B'].reset_index(drop=True)
df_TH_ATE = pd.merge(df_ATE_A,df_ATE_B,how='outer').reset_index(drop=True)
df_TH_ATE = df_TH_ATE[['Linkage Number','미착공수주잔']]
df_TH_ATE.to_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\ksmtest\test_TH.xlsx")
# Max 수량만큼 합치기
df_POWER['미착공수주잔']=0
df_POWER = df_POWER.astype({'Linkage Number':'str','No (*)':'str','Production Order':'str','Planned Order':'str'})
df_TH_PW = pd.concat([df_POWER,df_TH_ATE],join='outer',ignore_index=True)
df_TH_PW = df_TH_PW.astype({'Linkage Number':'str','No (*)':'str','Production Order':'str','Planned Order':'str'})
df_TH_PW.to_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\ksmtest\test_TH.xlsx",index=False)
>>>>>>> 288c337fe484a3713aab91d3959db03838749ef9
