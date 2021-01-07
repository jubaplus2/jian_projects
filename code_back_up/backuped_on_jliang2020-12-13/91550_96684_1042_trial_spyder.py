import pandas as pd
import numpy as ny
data= pd.read_excel("/home/jian/Projects/Big_Lots/TMR/Email_161002171230.xlsx",sheetname="weekly")
data['merge']=1
dma_pctg= pd.read_excel("/home/jian/Projects/Big_Lots/TMR/Email_161002171230.xlsx",sheetname="DMA pctg")
dma_pctg['merge']=1
data_dma=pd.merge(data,dma_pctg,on='merge',how='outer')
del data_dma['merge']
data_dma['DMA_cost']=data_dma['cost']*data_dma['Percentage']
data_dma.to_csv("/home/jian/Projects/Big_Lots/TMR/Email_161002171230_DMA_spyder.csv")