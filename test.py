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

#POWER만 선택
A_MAX = 50  #설정 최대생산대수
B_MAX = 50  #설정 최대생산대수
df_ATE_A = []
add = []
df_ATE_B = []
CNT_ATE_A = 0 
CNT_ATE_B = 0 
##################TEST######################
df_ATE_A = df_PowerSelect.loc[0:5]
add = df_PowerSelect.loc[6:7]
df_ATE_A = pd.merge(df_ATE_A,add,how='outer')
df_ATE_A = df_ATE_A.drop(7,axis=0)
df_ATE_A.to_excel(r"C:\Users\Administrator\Desktop\FAM3_Leveling-1\ksmtest\test1.xlsx")
print(df_ATE_A)
############################################
for i in range(len(str(df_PowerSelect['PRODUCT_TYPE'].index))):
    if CNT_ATE_A < A_MAX:
        CNT_ATE_A += df_PowerSelect['미착공수주잔'][i]  #최대수량까지 ADD
        add = df_PowerSelect.loc[i:i+1]
        add = add.drop(i+1,axis=0)
        df_ATE_A = pd.merge(df_ATE_A,add,how='outer')

    else:
        Save_BFCNT = A_MAX - CNT_ATE_A
        CNT_ATE_A = A_MAX
        
        df_addSmtAssy['미착공수주잔'][i] = Save_BFCNT

        break

for i in range(len(str(df_PowerSelect['PRODUCT_TYPE'].index))):
    if CNT_ATE_B < B_MAX:
        CNT_ATE_B += df_PowerSelect['미착공수주잔'][i]
    else:
        Save_BFCNT = B_MAX - CNT_ATE_B
        CNT_ATE_B = B_MAX
        df_addSmtAssy['미착공수주잔'][i] = Save_BFCNT
        break






