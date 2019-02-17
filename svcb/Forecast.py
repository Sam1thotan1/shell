# -*- coding: utf-8 -*-
"""
Created on Thu May 31 15:18:44 2018

@author: jiajuwu
"""
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime

from pyspark.sql.functions import dense_rank,lit,mean,udf
from pyspark.sql.window import Window
from pyspark.sql.types import *



def add_rnup_date(Repair_Date,Days_To_Next_Service_Up):
    if Repair_Date:
        Next_Service_Date_Up = datetime.strptime(str(Repair_Date), "%Y-%m-%d")+relativedelta(days=Days_To_Next_Service_Up.days)
    return Next_Service_Date_Up

def add_snup_date(Signature_Date,Repair_Date,Days_To_Next_Service_Up):
    if Signature_Date:
        Next_Service_Date_Up = Signature_Date+relativedelta(days=Days_To_Next_Service_Up.days)
    return Next_Service_Date_Up    
        
def add_rnlow_date(Repair_Date,Days_To_Next_Service_Low):
    if Repair_Date:
        Next_Service_Date_Low = datetime.strptime(str(Repair_Date), "%Y-%m-%d")+relativedelta(days=Days_To_Next_Service_Low.days)
    return Next_Service_Date_Low

def add_snlow_date(Signature_Date,Repair_Date,Days_To_Next_Service_Low):
    if Signature_Date:
        Next_Service_Date_Low = Signature_Date+relativedelta(days=Days_To_Next_Service_Low.days)
    return Next_Service_Date_Low  



class Prediction_Modelling:
    
    def __init__(self,spark_context):
        self.sc = spark_context
 
       
    #prepare mean day gap used as substituion for dayToNextService > 1000   
    def prepare_mdg(self,blue_cost_esse):
        day_gap = blue_cost_esse.select(['ContractNumber','Claimno','Repair_Date','DamageCode'])
        day_gap = day_gap.filter((day_gap.DamageCode == \
        '00012SV') | (day_gap.DamageCode == '00011SV'))
        day_gap_pd = day_gap.toPandas()
        day_gap_pd['SortOrder'] = day_gap_pd.groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')
        day_gap_pd['LaggedDate'] = day_gap_pd.groupby(['ContractNumber'])['Repair_Date'].shift(1)
        day_gap_pd['LaggedDC'] = day_gap_pd.groupby(['ContractNumber'])['DamageCode'].shift(1)
        day_gap_pd['LaggedSortOrder'] = day_gap_pd.groupby(['ContractNumber'])['SortOrder'].shift(1)
        
        day_gap_pd = day_gap_pd[~day_gap_pd.LaggedDate.isnull()]
        day_gap_pd = day_gap_pd[day_gap_pd.SortOrder <= day_gap_pd.LaggedSortOrder]
        day_gap_pd['DayGap'] = day_gap_pd['Repair_Date'] - day_gap_pd['LaggedDate']
        day_gap_pd['DayGap'] = day_gap_pd['DayGap'].apply(lambda x: x/np.timedelta64(1,'D'))
        day_gap_pd['Quantile'] = pd.qcut(day_gap_pd['DayGap'],[0, 0.05, 0.1, 0.25, 0.5, 0.75,0.95,1],labels=range(7))
        day_gap_pd = day_gap_pd[day_gap_pd['Quantile'] != 6]
        Mean_Day_Gap = day_gap_pd.DayGap.mean()
        
        return Mean_Day_Gap


    #forecast next service date    
    def forcast_nsd(self,blue_cost_detect,blue_cost_esse,blue_contract_esse):
        
        print("calculate mean day gap")
        Mean_Day_Gap = self.prepare_mdg(blue_cost_esse)
        
        print("filter contractnumber based on blue cost detect")
        #we need to check wether the above isin code need to transfer to toPandas
        blue_cost_detect_pd = blue_cost_detect.select(blue_cost_detect.ContractNumber)\
                                             .dropDuplicates(['ContractNumber'])
        blue_cost_detect_pd = blue_cost_detect_pd.withColumn('ContractNumber_filter',blue_cost_detect_pd.ContractNumber)
        blue_cost_esse = blue_cost_esse.join(blue_cost_detect_pd.select(\
                                        'ContractNumber','ContractNumber_filter'),on='ContractNumber',how='left')
        blue_cost_esse = blue_cost_esse.filter(blue_cost_esse.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')

        blue_cost_esse = blue_cost_esse.withColumnRenamed('Service_Type','Product_Line')
        
        print("filter damage code 00012SV/00011SV")
        blue_cost_ab = blue_cost_esse.filter((blue_cost_esse.DamageCode == \
        '00012SV') | (blue_cost_esse.DamageCode == '00011SV'))\
                                              .select(['ContractNumber'
                                                       ,'Claimno'
                                                       ,'Product_EN'
                                                       ,'Product_Line'
                                                       ,'DamageCode'
                                                       ,'Repair_Date'
                                                       ,'First_Registration_Date'
                                                       ,'Repair_Age'
                                                       ,'Mileage_When_Repair'
                                                       ,'Mileage_Per_Day'])

   
        print("generate blue cost inservice")              
        #we need to check wether the above isin code need to transfer to toPandas 
        blue_contract_pd = blue_contract_esse.select(['ContractNumber','Status'])
        blue_esse_contracnum_ins =  blue_contract_esse.filter(blue_contract_esse.Status == 'In-Service').dropDuplicates(['ContractNumber'])
        blue_esse_contracnum_ins = blue_esse_contracnum_ins.withColumn('ContractNumber_filter', blue_esse_contracnum_ins.ContractNumber) 
        blue_cost_inservice = blue_cost_ab.join(blue_esse_contracnum_ins.select('ContractNumber','ContractNumber_filter'),'ContractNumber','left') 
        blue_cost_inservice = blue_cost_inservice.filter(blue_cost_inservice.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')
   
        print("generate blue cost end")  
        blue_esse_contracnum_end = blue_contract_pd[blue_contract_pd.Status == 'Normally End'].dropDuplicates(['ContractNumber'])        
        blue_esse_contracnum_end = blue_esse_contracnum_end.withColumn('ContractNumber_filter',blue_esse_contracnum_end.ContractNumber)
        blue_cost_ab = blue_cost_ab.join(blue_esse_contracnum_end.select(\
                                        'ContractNumber','ContractNumber_filter'),on='ContractNumber',how='left')        
        blue_cost_end = blue_cost_ab.filter(blue_cost_ab.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')
        
        print("generate sortorder column for in service")
        blue_cost_inservice = blue_cost_inservice.toPandas()
        blue_cost_inservice['SortOrder']= (blue_cost_inservice[['ContractNumber',
                                          'Claimno',
                                          'Repair_Date']]).groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')


        print("generate sortorder column for end service")
        blue_cost_end = blue_cost_end.toPandas()
        blue_cost_end['SortOrder']= (blue_cost_end[['ContractNumber',
                                                  'Claimno',
                                                  'Repair_Date']]).groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')


        
        blue_cost_latest1 = blue_cost_inservice[blue_cost_inservice.SortOrder == 1]

        print("calculate days to next service up/low")
        blue_cost_latest1['Days_To_Next_Service_Up'] = 12000/blue_cost_latest1['Mileage_Per_Day']
        blue_cost_latest1['Days_To_Next_Service_Low'] = 8000/blue_cost_latest1['Mileage_Per_Day']
        
        blue_cost_latest1.loc[blue_cost_latest1.Days_To_Next_Service_Up > 1000, 'Days_To_Next_Service_Up'] = Mean_Day_Gap
        blue_cost_latest1.loc[blue_cost_latest1.Days_To_Next_Service_Low > 1000, 'Days_To_Next_Service_Low'] = Mean_Day_Gap
        
        blue_cost_latest1['Days_To_Next_Service_Up'] = blue_cost_latest1['Days_To_Next_Service_Up'].astype(int)
        blue_cost_latest1['Days_To_Next_Service_Low'] = blue_cost_latest1['Days_To_Next_Service_Low'].astype(int)
        
        blue_cost_latest2 = blue_cost_end[blue_cost_end.SortOrder == 1]
        
        print("calculate avg das up/low")
        AVG_Mileage = blue_cost_latest2.Mileage_Per_Day.mean()
        AVG_Days_Up = 12000/AVG_Mileage
        AVG_Days_Low = 8000/AVG_Mileage


        print('merge blue cost latest1 with blue_contract_esse_pd')
        blue_contract_esse_pd = blue_contract_esse.select(['ContractNumber',\
        'Status','Signature_Date']).toPandas()
        fnxd_pd = blue_contract_esse_pd.merge(blue_cost_latest1[['ContractNumber',\
        'Days_To_Next_Service_Up','Days_To_Next_Service_Low','Repair_Date']],\
        on='ContractNumber',how='left')
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Days_To_Next_Service_Up'] = AVG_Days_Up
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Days_To_Next_Service_Low'] = AVG_Days_Low
        fnxd_pd['Days_To_Next_Service_Up'] = fnxd_pd['Days_To_Next_Service_Up'] .fillna(0)
        fnxd_pd['Days_To_Next_Service_Low'] = fnxd_pd['Days_To_Next_Service_Low'] .fillna(0)
        fnxd_pd['Days_To_Next_Service_Up'] = fnxd_pd['Days_To_Next_Service_Up'].astype(int)
        fnxd_pd['Days_To_Next_Service_Low'] = fnxd_pd['Days_To_Next_Service_Low'].astype(int)        
        fnxd_pd['Days_To_Next_Service_Up'] = pd.to_timedelta(fnxd_pd['Days_To_Next_Service_Up'],\
               unit='D', errors='coerce')
        fnxd_pd['Days_To_Next_Service_Low'] = pd.to_timedelta(fnxd_pd['Days_To_Next_Service_Low'],\
               unit='D', errors='coerce')

        
        print("calculate next service date")   
        fnxd_pd.loc[fnxd_pd.Status == 'In-Service', 'Next_Service_Date_Up'] =  \
                   fnxd_pd.loc[(fnxd_pd.Status == 'In-Service') & (~fnxd_pd.Repair_Date.isnull()),]\
                               .apply(lambda x: add_rnup_date(x.Repair_Date,x.Days_To_Next_Service_Up),axis=1)   
    
        fnxd_pd.loc[fnxd_pd.Status == 'In-Service', 'Next_Service_Date_Low'] = \
                   fnxd_pd.loc[(fnxd_pd.Status == 'In-Service') & (~fnxd_pd.Repair_Date.isnull()),]\
                              .apply(lambda x: add_rnlow_date(x.Repair_Date,x.Days_To_Next_Service_Low),axis=1)                                 
    
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Next_Service_Date_Up'] = \
                   fnxd_pd.loc[fnxd_pd.Status == 'Not-start']\
                              .apply(lambda x: add_snup_date(x.Signature_Date,x.Days_To_Next_Service_Up),axis=1)   
    
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Next_Service_Date_Low'] = \
                   fnxd_pd.loc[fnxd_pd.Status == 'Not-start']\
                              .apply(lambda x: add_snlow_date(x.Signature_Date,x.Days_To_Next_Service_Low),axis=1) 
    
        fnxd_pd['Next_Service_Date_Low'] = fnxd_pd['Next_Service_Date_Low'].astype(str)
        fnxd_pd['Next_Service_Date_Up'] = fnxd_pd['Next_Service_Date_Up'].astype(str)
        fnxd_pd['Repair_Date'] = fnxd_pd['Repair_Date'].astype(str)        
    

        print("convert to pyspark dataframe")
        schema = StructType([StructField('ContractNumber', StringType(), True)
                            ,StructField('Days_To_Next_Service_Up', StringType(), True)
                            ,StructField('Days_To_Next_Service_Low', StringType(), True)
                            ,StructField('Next_Service_Date_Low', StringType(), True)
                            ,StructField('Next_Service_Date_Up', StringType(), True)])
    
        fnxd_df = spark.createDataFrame(fnxd_pd[['ContractNumber',\
        'Days_To_Next_Service_Low','Days_To_Next_Service_Up',\
        'Next_Service_Date_Low','Next_Service_Date_Up']], schema)        
        
        
        print("generate blue contract final")
        blue_contract_esse = blue_contract_esse.join(blue_cost_esse.select('ContractNumber','Violate_Status'),'ContractNumber','left')
        blue_contract_final = blue_contract_esse.join(fnxd_df,'ContractNumber','left')

        
        print("Load data in table tmp_blue_contract_upperlimit_info")
        blue_contract_final.registerTempTable("blue_contract_final")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_blue_contract_final_info ")
        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_blue_contract_final_info  select * from blue_contract_final")        

        return blue_contract_final
        

    def isp_prepare_mdg(self,isp_blue_cost_esse):
        day_gap = isp_blue_cost_esse.select(['ContractNumber','Claimno','Repair_Date','DamageCode'])
        day_gap = day_gap.filter((day_gap.DamageCode == \
        '00012SV') | (day_gap.DamageCode == '00011SV'))
        day_gap_pd = day_gap.toPandas()
        day_gap_pd['SortOrder'] = day_gap_pd.groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')
        day_gap_pd['LaggedDate'] = day_gap_pd.groupby(['ContractNumber'])['Repair_Date'].shift(1)
        day_gap_pd['LaggedDC'] = day_gap_pd.groupby(['ContractNumber'])['DamageCode'].shift(1)
        day_gap_pd['LaggedSortOrder'] = day_gap_pd.groupby(['ContractNumber'])['SortOrder'].shift(1)
        day_gap_pd = day_gap_pd[~day_gap_pd.LaggedDate.isnull()]
        day_gap_pd = day_gap_pd[day_gap_pd.SortOrder <= day_gap_pd.LaggedSortOrder]
        day_gap_pd['DayGap'] = day_gap_pd['Repair_Date'] - day_gap_pd['LaggedDate']
        day_gap_pd['DayGap'] = day_gap_pd['DayGap'].apply(lambda x: x/np.timedelta64(1,'D'))
        day_gap_pd['Quantile'] = pd.qcut(day_gap_pd['DayGap'],[0, 0.05, 0.1, 0.25, 0.5, 0.75,0.95,1],labels=range(7))
        day_gap_pd = day_gap_pd[day_gap_pd['Quantile'] != 6]
        isp_Mean_Day_Gap = day_gap_pd.DayGap.mean()
        
        return isp_Mean_Day_Gap


    #forecast next service date    
    def isp_forcast_nsd(self,blue_cost_detect,isp_blue_cost_esse,blue_contract_esse):
        
        print("calculate mean day gap")
        isp_Mean_Day_Gap = self.prepare_mdg(blue_cost_esse)
        
        print("filter contractnumber based on blue cost detect")
        #we need to check wether the above isin code need to transfer to toPandas 
        isp_blue_cost_detect_pd = isp_blue_cost_detect.select(isp_blue_cost_detect.ContractNumber)\
                                             .dropDuplicates(['ContractNumber'])
        isp_blue_cost_detect_pd = isp_blue_cost_detect_pd.withColumn('ContractNumber_filter',isp_blue_cost_detect_pd.ContractNumber)
        isp_blue_cost_esse = isp_blue_cost_esse.join(isp_blue_cost_detect_pd.select(\
                                        'ContractNumber','ContractNumber_filter'),on='ContractNumber',how='left')
        isp_blue_cost_esse = isp_blue_cost_esse.filter(isp_blue_cost_esse.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')

        isp_blue_cost_esse = isp_blue_cost_esse.withColumnRenamed('Service_Type','Product_Line')
        
        print("filter damage code 00012SV/00011SV")
        isp_blue_cost_ab = isp_blue_cost_esse.filter((isp_blue_cost_esse.DamageCode == \
        '00012SV') | (isp_blue_cost_esse.DamageCode == '00011SV'))\
                                              .select(['ContractNumber'
                                                       ,'Claimno'
                                                       ,'Product_EN'
                                                       ,'Product_Line'
                                                       ,'DamageCode'
                                                       ,'Repair_Date'
                                                       ,'First_Registration_Date'
                                                       ,'Repair_Age'
                                                       ,'Mileage_When_Repair'
                                                       ,'Mileage_Per_Day'])

   
        print("generate blue cost inservice")        
        #we need to check wether the above isin code need to transfer to toPandas 
        isp_blue_contract_pd = isp_blue_contract_esse.select(['ContractNumber','Status'])
        isp_blue_esse_contracnum_ins =  isp_blue_contract_esse.filter(isp_blue_contract_esse.Status == 'In-Service').dropDuplicates(['ContractNumber'])
        isp_blue_esse_contracnum_ins = isp_blue_esse_contracnum_ins.withColumn('ContractNumber_filter', isp_blue_esse_contracnum_ins.ContractNumber)
        isp_blue_cost_inservice = isp_blue_cost_ab.join(isp_blue_esse_contracnum_ins.select('ContractNumber','ContractNumber_filter'),'ContractNumber','left') 
        isp_blue_cost_inservice = isp_blue_cost_inservice.filter(isp_blue_cost_inservice.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')
   
        print("generate isp_blue cost end")
        isp_blue_esse_contracnum_end = isp_blue_contract_pd[isp_blue_contract_pd.Status == 'Normally End'].dropDuplicates(['ContractNumber'])        
        isp_blue_esse_contracnum_end = isp_blue_esse_contracnum_end.withColumn('ContractNumber_filter',isp_blue_esse_contracnum_end.ContractNumber)
        isp_blue_cost_ab = isp_blue_cost_ab.join(isp_blue_esse_contracnum_end.select(\
                                        'ContractNumber','ContractNumber_filter'),on='ContractNumber',how='left')        
        isp_blue_cost_end = isp_blue_cost_ab.filter(isp_blue_cost_ab.ContractNumber_filter.isNotNull()).drop('ContractNumber_filter')
 
        print("generate sortorder column for in service")
        isp_blue_cost_inservice = isp_blue_cost_inservice.toPandas()
        isp_blue_cost_inservice['SortOrder']= (isp_blue_cost_inservice[['ContractNumber',
                                          'Claimno',
                                          'Repair_Date']]).groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')


        print("generate sortorder column for end service")
        isp_blue_cost_end = isp_blue_cost_end.toPandas()
        isp_blue_cost_end['SortOrder']= (isp_blue_cost_end[['ContractNumber',
                                                  'Claimno',
                                                  'Repair_Date']]).groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')


        
        isp_blue_cost_latest1 = isp_blue_cost_inservice[isp_blue_cost_inservice.SortOrder == 1]

        print("calculate days to next service up/low")
        isp_blue_cost_latest1['Days_To_Next_Service_Up'] = 12000/isp_blue_cost_latest1['Mileage_Per_Day']
        isp_blue_cost_latest1['Days_To_Next_Service_Low'] = 8000/isp_blue_cost_latest1['Mileage_Per_Day']
        
        isp_blue_cost_latest1.loc[isp_blue_cost_latest1.Days_To_Next_Service_Up > 1000, 'Days_To_Next_Service_Up'] = isp_Mean_Day_Gap
        isp_blue_cost_latest1.loc[isp_blue_cost_latest1.Days_To_Next_Service_Low > 1000, 'Days_To_Next_Service_Low'] = isp_Mean_Day_Gap
        
        isp_blue_cost_latest1['Days_To_Next_Service_Up'] = isp_blue_cost_latest1['Days_To_Next_Service_Up'].astype(int)
        isp_blue_cost_latest1['Days_To_Next_Service_Low'] = isp_blue_cost_latest1['Days_To_Next_Service_Low'].astype(int)   
        isp_blue_cost_latest2 = isp_blue_cost_end[isp_blue_cost_end.SortOrder == 1]
        
        print("calculate avg das up/low")
        AVG_Mileage = isp_blue_cost_latest2.Mileage_Per_Day.mean()
        AVG_Days_Up = 12000/AVG_Mileage
        AVG_Days_Low = 8000/AVG_Mileage
      
        print('merge isp_blue cost latest1 with isp_blue_contract_esse_pd')
        isp_blue_contract_esse_pd = isp_blue_contract_esse.select(['ContractNumber',\
        'Status','Signature_Date']).toPandas()
        fnxd_pd = isp_blue_contract_esse_pd.merge(isp_blue_cost_latest1[['ContractNumber',\
        'Days_To_Next_Service_Up','Days_To_Next_Service_Low','Repair_Date']],\
        on='ContractNumber',how='left')
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Days_To_Next_Service_Up'] = AVG_Days_Up
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Days_To_Next_Service_Low'] = AVG_Days_Low
        fnxd_pd['Days_To_Next_Service_Up'] = fnxd_pd['Days_To_Next_Service_Up'] .fillna(0)
        fnxd_pd['Days_To_Next_Service_Low'] = fnxd_pd['Days_To_Next_Service_Low'] .fillna(0)
        fnxd_pd['Days_To_Next_Service_Up'] = fnxd_pd['Days_To_Next_Service_Up'].astype(int)
        fnxd_pd['Days_To_Next_Service_Low'] = fnxd_pd['Days_To_Next_Service_Low'].astype(int)        
        fnxd_pd['Days_To_Next_Service_Up'] = pd.to_timedelta(fnxd_pd['Days_To_Next_Service_Up'],\
               unit='D', errors='coerce')
        fnxd_pd['Days_To_Next_Service_Low'] = pd.to_timedelta(fnxd_pd['Days_To_Next_Service_Low'],\
               unit='D', errors='coerce')

        
        
        print("calculate next service date")   
        fnxd_pd.loc[fnxd_pd.Status == 'In-Service', 'Next_Service_Date_Up'] =  \
                   fnxd_pd.loc[(fnxd_pd.Status == 'In-Service') & (~fnxd_pd.Repair_Date.isnull()),]\
                               .apply(lambda x: add_rnup_date(x.Repair_Date,x.Days_To_Next_Service_Up),axis=1)   
    
        fnxd_pd.loc[fnxd_pd.Status == 'In-Service', 'Next_Service_Date_Low'] = \
                   fnxd_pd.loc[(fnxd_pd.Status == 'In-Service') & (~fnxd_pd.Repair_Date.isnull()),]\
                              .apply(lambda x: add_rnlow_date(x.Repair_Date,x.Days_To_Next_Service_Low),axis=1)                                 
    
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Next_Service_Date_Up'] = \
                   fnxd_pd.loc[fnxd_pd.Status == 'Not-start']\
                              .apply(lambda x: add_snup_date(x.Signature_Date,x.Days_To_Next_Service_Up),axis=1)   
    
        fnxd_pd.loc[fnxd_pd.Status == 'Not-Start', 'Next_Service_Date_Low'] = \
                   fnxd_pd.loc[fnxd_pd.Status == 'Not-start']\
                              .apply(lambda x: add_snlow_date(x.Signature_Date,x.Days_To_Next_Service_Low),axis=1) 
    
        fnxd_pd['Next_Service_Date_Low'] = fnxd_pd['Next_Service_Date_Low'].astype(str)
        fnxd_pd['Next_Service_Date_Up'] = fnxd_pd['Next_Service_Date_Up'].astype(str)
        fnxd_pd['Repair_Date'] = fnxd_pd['Repair_Date'].astype(str)        
    

        print("convert to pyspark dataframe")
        schema = StructType([StructField('ContractNumber', StringType(), True)
                            ,StructField('Days_To_Next_Service_Up', StringType(), True)
                            ,StructField('Days_To_Next_Service_Low', StringType(), True)
                            ,StructField('Next_Service_Date_Low', StringType(), True)
                            ,StructField('Next_Service_Date_Up', StringType(), True)])
    
        fnxd_df = spark.createDataFrame(fnxd_pd[['ContractNumber',\
        'Days_To_Next_Service_Low','Days_To_Next_Service_Up',\
        'Next_Service_Date_Low','Next_Service_Date_Up']], schema)        
        
        
        print("generate isp_blue contract final")
        isp_blue_contract_final = isp_blue_contract_esse.join(fnxd_df,'ContractNumber','left')

        
        print("Load data in table tmp_isp_blue_contract_upperlimit_info")
        isp_blue_contract_final.registerTempTable("isp_blue_contract_final")
        self.sc.sql("truncate table revr_bmbs_srv_contract_isp_blue.tmp_isp_blue_contract_final_info ")
        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_isp_blue.tmp_isp_blue_contract_final_info  select * from isp_blue_contract_final")        

        return isp_blue_contract_final 

































        
