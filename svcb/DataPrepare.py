# -*- coding: utf-8 -*-
"""
Created on Fri May 11 15:32:45 2018

@author: jiajuwu and junyi
"""

from pyspark.sql.types import *
from pyspark.sql.functions import udf,isnull,month,year,dense_rank,lit,concat_ws,round
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from datetime import datetime
import pandas as pd
import numpy as np

#import svcb.ETL as etl

def fillna_gssn(gssn_old,gssn_new):
    if gssn_new:
        gssn_new_changed = gssn_new
    else:
        gssn_new_changed = gssn_old
    
    return gssn_new_changed


def label_violate_status(vehicle_age):
    if vehicle_age >= 1080:
        violate_status = 'Not Violate'
    else:
        violate_status = 'Violate'
    return  violate_status  

      
def get_detail_label(standard_val, actual_val,Service):
    """
    generate diffirence of service details between standard value and actual value
    based on each contract
    standard_val refers to standard value of each service 
    actual_val refers to actual value of each service 
    service refers to services, including A,B,A/C,SP (stringType)
    """
    diff = standard_val - actual_val
    
    if (diff > 0):
        Detail = str(int(abs(diff))) + 'More' + Service
    elif (diff < 0):
       Detail = str(int(abs(diff))) + 'Less' + Service
    else:
        Detail = "Equal"    

    return Detail


def get_remainservice(dccount,servicetimes):
    
    if dccount and servicetimes:
        remainservice = abs(dccount - servicetimes)
    else:
        remainservice = ''
    
    return remainservice



def get_status(RemainService,Detail,ServiceTimes):
    """
    generate status label    
    """    
    if RemainService:
        if float(RemainService) == 0:
            Status = 'Normally End'
        elif float(RemainService) > 0 and Detail.find('More') > 0  and ServiceTimes != 0:
            Status = 'Potential Risk'
        elif float(RemainService) > 0 and Detail.find('More') < 0:
            Status = 'In-Service'
        else:
            Status = ''
    else:
        if ServiceTimes == 0:
            Status = 'Not-Start'
        else:
            Status = ''

    return Status  

def lable_isp_blue_status(line,signature,repair,mileage,remain,service):
#    now_time = datetime.datetime.now()
    now_time = datetime.now().date()
    if line == '3yr/40k':
        if (remain ==0)|((now_time-signature).days == 1080)|(mileage==40000):
            status='Normally End'
        elif (mileage>40000)|(remain<0):
            if pd.notnull(repair)&((repair-signature).days > 1080):
                status='Over Limit'
            else:
                status='Over Limit'                
        elif (remain > 0)&(service >0):             
            status='In-Service'
        elif (remain > 0)&(service ==0)&((now_time-signature).days < 1080)&(pd.isnull(mileage)):             
            status='Not-Start'
        elif ((remain > 0)&(service ==0))&((now_time-signature).days < 1080)&(mileage < 40000):             
            status='Not-Start'
        else:
            status='Not-Start'

    elif line == '1yr/10k':
        if (remain ==0)|((now_time-signature).days == 360)|(mileage==10000):
            status='Normally End'
        elif (mileage>10000)|(remain<0):
            if pd.notnull(repair)&((repair-signature).days > 360):
                status='Over Limit'
            else:
                status='Over Limit'                    
        elif (remain > 0)&(service >0):             
            status='In-Service'
        elif (remain > 0)&(service ==0)&((now_time-signature).days < 360)&(pd.isnull(mileage)):          
            status='Not-Start'            
        elif ((remain > 0)&(service ==0))&((now_time-signature).days < 360)&(mileage < 10000):             
            status='Not-Start'
        else:
            status='Not-Start'

    elif (line.find("A")!=-1)|(line.find("B")!=-1):
        if (remain == 0):
            status='Normally End'
        elif (remain < 0):            
            status='Over Limit'            
        elif (remain > 0)&(service >0):            
            status='In-Service'
        elif ((remain > 0)&(service ==0)):             
            status='Not-Start'
        else:
            status='Not-Start'            
    else:
        status='Other_Product'            
    return status    


def lable_silver_status(end,repair_date):
#    now_time = datetime.now()
    now_time = datetime.now().date()

    if repair_date:
        if now_time<end:
            status='In-Service'
        elif repair_date > end:
            status='Over Limit'     
        else:
            status='Normally End'
    else:
        if now_time>=end:
            status='Normally End'
        else:
            status='Not-Start'
    return status        

  
class Prepare_Blue_Info:

    def __init__(self,spark_context):
        self.sc = spark_context
    
    def _get_blue_overview(self,contract_info,product_blue_info,cost_info,\
                           dealer_gssn,dealer_workshop):
        """
        create table blue contract and cost
        """
        #create table blue contract
        product_blue_info = product_blue_info.withColumnRenamed('Brand','Brand_P')
        blue_contract = contract_info.join(product_blue_info, 'Vega_Code', 'inner')\
                                          .join(dealer_gssn, 'GSSN', 'left')
                                          
        blue_contract = blue_contract.filter((blue_contract.Baumuster_6 != '205343') & (blue_contract.Baumuster_6 != '204052'))
        blue_contract = blue_contract.filter(~blue_contract.GSSN.like("%/%"))
                                 
        #create table blue cost
        blue_cost = cost_info.join(product_blue_info, 'Vega_Code', 'inner')\
                                  .join(dealer_workshop, 'Workshop', 'left')

        #rename columns
        rename_cols = {'Dealername_CN':'ServiceDealer_CN', 
                            'Region': 'ServiceRegion',
                            'OwnerGroup':'ServiceGroup'}
        
        for col in rename_cols:
            blue_cost = blue_cost.withColumnRenamed(col, rename_cols[col])    

        return blue_contract, blue_cost

        
                       
    def _get_blue_esse(self,blue_contract,blue_cost):
        
        """
        Part 1: blue contract_esse and cost_esse 
        (including blue and blue light product)
        """
        
        #select columns
        blue_contract_essence = blue_contract.select(['ContractNumber',
                                                     'Fin',
                                                     'Baumuster_6',
                                                     'Brand',
                                                     'Class',
                                                     'Model',
                                                     'First_Registration_Date',
                                                     'Signature_Date',
                                                     'Vehicle_Age',
                                                     'Product_EN',
                                                     'Product',
                                                     'Value',
                                                     'Oil',
                                                     'CGI',
                                                     'DCCount',
                                                     'A_Count',
                                                     'B_Count',
                                                     'A_CFilter_Count',
                                                     'Spark_Plug_Count',
                                                     'GSSN',
                                                     'Dealer_Name_CN',
                                                     'Owner_Group',
                                                     'Region',
                                                     'Province',
                                                     'City',
                                                     'Service_Type'])

        blue_cost = blue_cost.join(blue_contract.select('ContractNumber','Vehicle_Age'),'ContractNumber','left')
        func_label_violate_status = udf(label_violate_status, StringType())
        blue_cost = blue_cost.withColumn('Violate_Status', func_label_violate_status('Vehicle_Age'))
 
        blue_cost_essence = blue_cost.select(['ContractNumber',
                                              'Claimno',
                                              'Product_EN',
                                              'Product',
                                              'DamageCode',
                                              'Claim_Amount',
                                              'Claim_Date',
                                              'Repair_Date',
                                              'First_Registration_Date',
                                              'Vegacredit_Date',
                                              'Mileage_When_Repair',
                                              'Repair_Age',
                                              'Mileage_Per_Day',
                                              'Workshop',
                                              'Dealer_Name_CN',
                                              'Owner_Group',
                                              'ServiceRegion',
                                              'Province',
                                              'City',
                                              'Service_Type',
                                              'Violate_Status'])
        
        blue_cost_essence = blue_cost_essence.filter(blue_cost_essence.Repair_Age >= 90)            

        """
        Calculate service times except damagecode:2103601
        """
        # the cost of  '2103601' Damagecode just occupy a little part of total repair fee, as a result, we ignore it
        blue_cost_essence2 = blue_cost_essence.filter(blue_cost_essence.DamageCode != '2103601')
        blue_cost_essence2 = blue_cost_essence2.filter(blue_cost_essence2.DamageCode.substr(1, 1)!='2')
        
        
        #calculate out the amount of Damagecode under each ContractNumber,
        blue_count = blue_cost_essence2.groupby('ContractNumber').agg({'DamageCode':'count'})
        
        #after use agg fuction, the result of counting Damagecode column it will generate new column 
        #and it will be 'count(Damagecode)',so we need to rename it
        blue_count = blue_count.withColumnRenamed('count(DamageCode)','ServiceTimes')
        
        #count items  of the damagecode is in '00012SV','00011SV','83108SV','15031SV'
        blue_count_A = blue_cost_essence2.filter(blue_cost_essence2.DamageCode == '00012SV')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        blue_count_A = blue_count_A.withColumnRenamed('count(DamageCode)','A')
        
        blue_count_B = blue_cost_essence2.filter(blue_cost_essence2.DamageCode == '00011SV')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        blue_count_B = blue_count_B.withColumnRenamed('count(DamageCode)','B')
        
        
        blue_count_ac = blue_cost_essence2.filter(blue_cost_essence2.DamageCode == '83108SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        blue_count_ac = blue_count_ac.withColumnRenamed('count(DamageCode)','A/C')
        
        blue_count_sp = blue_cost_essence2.filter(blue_cost_essence2.DamageCode == '15031SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        blue_count_sp = blue_count_sp.withColumnRenamed('count(DamageCode)','SP')
        
        #add those 5 new table into blue_count in order to to know the total num and each mount of those four damagecode
        blue_count = blue_count.join(blue_count_A, 'ContractNumber', how = 'left')\
                               .join(blue_count_B, 'ContractNumber', how = 'left')\
                               .join(blue_count_ac, 'ContractNumber', how = 'left')\
                               .join(blue_count_sp, 'ContractNumber', how = 'left')
        
        #use 0 to repalce nan value
        blue_count=blue_count.na.fill(0)
        
        """
        determine the contract status
        """
        blue_contract_essence = blue_contract_essence.join(blue_count, 'ContractNumber', how = 'left')
        
        #use 0 to repalce nan value
        fill_na_list = ['ServiceTimes','A','B','A/C','SP']
        fill_na_dict = {i:0 for i in fill_na_list}
        blue_contract_essence = blue_contract_essence.na.fill(fill_na_dict)
                
        """
        generate service detail label column
        """
        func_get_detail_label = udf(get_detail_label, StringType())
        services = {'A':'A_Count','B':'B_Count','A/C':'A_Cfilter_Count','SP':'Spark_Plug_Count'}
        for i in services:
            blue_contract_essence = blue_contract_essence.withColumn(i+'_col',lit(i))
        
            blue_contract_essence = blue_contract_essence.withColumn('detail_'+i, \
                                                                     func_get_detail_label(services[i],\
                                                                     i,i+'_col'))
        
        blue_contract_essence = blue_contract_essence.drop('A_col','B_col','A/C_col','SP_col')
                    
        """
        generate remainservice column
        """
        func_get_remainservice = udf(get_remainservice,StringType())
        blue_contract_essence = blue_contract_essence.withColumn('RemainService', \
                                                                 func_get_remainservice(blue_contract_essence.DCCount,\
                                                                  blue_contract_essence.ServiceTimes))

        """
        generate status column
        """
        blue_contract_essence = blue_contract_essence.withColumn('Total_Detail',\
                                                                 concat_ws(' ',blue_contract_essence['Detail_A'],\
                                                                           blue_contract_essence['Detail_B'],\
                                                                           blue_contract_essence['Detail_A/C'],\
                                                                           blue_contract_essence['Detail_SP']))
        
        func_get_status = udf(get_status, StringType())
        blue_contract_essence = blue_contract_essence.withColumn('Status', \
                                                                 func_get_status('RemainService','Total_Detail','ServiceTimes'))
                
        
        """
        calculate cost by contract/claim
        """     
        blue_costbycontract = blue_cost_essence.groupby('ContractNumber')\
                                                       .agg({'Claim_Amount':'sum'})
        blue_costbycontract = blue_costbycontract.withColumnRenamed('Sum(Claim_Amount)','CostTillNow')
        
        #merge back to contract table        
        blue_contract_essence = blue_contract_essence.join(blue_costbycontract, 'ContractNumber', 'left')
        #repalce na with 0 in costTillNow column
        blue_contract_essence = blue_contract_essence.na.fill({'CostTillNow': 0})
        
        """
        generate balance column
        """
        blue_contract_essence = blue_contract_essence.withColumn('Balance',\
                                                                 (blue_contract_essence.Value - \
                                                                  blue_contract_essence.CostTillNow))

        print("Load data in table tmp_blue_contract_essence_info")
        blue_contract_essence.registerTempTable("blue_contract_esse")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_blue_contract_essence_info")
        spark.sql("insert into table revr_bmbs_srv_contract_blue.tmp_blue_contract_essence_info select * from blue_contract_esse") 
        
        
        return blue_contract_essence,blue_cost_essence
    
    
    #
    def _get_blue_detect(self,blue_cost):
        
        blue_cost_detect = blue_cost.select(['ContractNumber','Claimno','Repair_Date','DamageCode'])
        blue_cost_detect = blue_cost_detect.filter((blue_cost_detect.DamageCode == '00012SV') | \
                                                   (blue_cost_detect.DamageCode == '00011SV'))
        
        #create sortorder column
        blue_cost_detect_pd = blue_cost_detect.toPandas()
        blue_cost_detect_pd['Sortorder'] = blue_cost_detect_pd.groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')
        
        blue_cost_detect_pd['LaggedDate'] = blue_cost_detect_pd.groupby(['ContractNumber'])['Repair_Date'].shift(1)
        blue_cost_detect_pd['LaggedDC'] = blue_cost_detect_pd.groupby(['ContractNumber'])['DamageCode'].shift(1)
        blue_cost_detect_pd['LaggedSortOrder'] = blue_cost_detect_pd.groupby(['ContractNumber'])['Sortorder'].shift(1)
        blue_cost_detect_pd = blue_cost_detect_pd[-blue_cost_detect_pd.LaggedDate.isnull()]
        blue_cost_detect_pd = blue_cost_detect_pd[blue_cost_detect_pd.Sortorder <= blue_cost_detect_pd.LaggedSortOrder]
        blue_cost_detect_pd['DayGap'] = blue_cost_detect_pd['Repair_Date'] - blue_cost_detect_pd['LaggedDate'] 
        blue_cost_detect_pd['DayGap'] = blue_cost_detect_pd['DayGap'].apply(lambda x: x/np.timedelta64(1,'D'))
        blue_cost_detect_pd = blue_cost_detect_pd[blue_cost_detect_pd.DayGap < 10]

        #in testing, please verify the column name of claimno
        #and check the data type of column related to date
        schema = StructType([StructField("ContractNumber", StringType(), True)
                             ,StructField("Claimno", StringType(), True)
                             ,StructField("RepairDate", DateType(), True)
                             ,StructField("DamageCode", StringType(), True)
                             ,StructField("SortOrder", FloatType(), True)
                             ,StructField("LaggedDate", DateType(), True)
                             ,StructField("LaggedDC", StringType(), True)
                             ,StructField("LaggedSortOrder", FloatType(), True)
                             ,StructField("DayGap", FloatType(), True)])


        blue_cost_detect = self.sc.createDataFrame(blue_cost_detect_pd, schema)
        
        print("Load data in table tmp_blue_cost_detect_info")
        blue_cost_detect.registerTempTable("blue_detect")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_blue_cost_detect_info")
        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_blue_cost_detect_info select * from blue_detect")       
                
        
        return blue_cost_detect
        
        

def get_service_type(contractduration):
    """
    service_type logic:
        if contractduration == 1440 or cn.contractduration == 1442 then '3+1' 
        if contractduration >= 1800 then '3+1+1' 
        if contractduration == 360 then '3+2
    """        
    if contractduration == 1440 or contractduration == 1442:
        Service_Type = '3+1'
    elif contractduration == 360:
        Service_Type = '3+1+1'
    else:
        Service_Type = '3+2'
    
    return Service_Type


class Prepare_Silver_Info:

    def __init__(self,spark_context):
        self.sc = spark_context
    
    def _get_silver_overview(self,contract_info,product_silver_info,cost_info,\
                             dealer_gssn,dealer_workshop):
        """
        create table silver contract and cost
        """
        #product silver
        silver_contract = contract_info.join(product_silver_info, 'Product', 'inner')
        silver_contract = silver_contract.filter(~silver_contract.GSSN.like("%/%"))
         
        #update silver contract_ duration and filter it 
        silver_contract = silver_contract.replace(['1799'],['1800'],'Contract_Duration')
        
        silver_contract_icon2 = silver_contract.filter(silver_contract.SourceSystem =='iCON2')
        silver_contract_no_icon2 = silver_contract.filter(silver_contract.SourceSystem !='iCON2')
        
        silver_contract_icon2 = silver_contract_icon2.filter((silver_contract_icon2.ContractDuration == 1440) | \
                       (silver_contract_icon2.ContractDuration == 1442) | (silver_contract_icon2.ContractDuration == 360) | \
                       (silver_contract_icon2.ContractDuration >= 1800) |(silver_contract_icon2.ContractDuration == 720))
        
        silver_contract_no_icon2 = silver_contract_no_icon2.filter((silver_contract_no_icon2.ContractDuration == 1440) | \
                       (silver_contract_no_icon2.ContractDuration == 1442) | (silver_contract_no_icon2.ContractDuration == 360) | \
                       (silver_contract_no_icon2.ContractDuration >= 1800))

        silver_contract=silver_contract_icon2.unionAll(silver_contract_no_icon2)     

        silver_cost = cost_info.join(product_silver_info, 'Product', 'inner')

        #generate service_type column and merge table dealer_gssn
        func_get_service_type = udf(get_service_type, StringType())
        silver_contract = silver_contract.withColumn('Service_Type',func_get_service_type('Contract_Duration'))\
                                                    .join(dealer_gssn, 'GSSN', 'left')

        #use service_type in table contract to cover service_type in table cost
        #merge table dealer_workshop
        silver_cost = silver_cost.drop('Service_Type')
        silver_cost = silver_cost.join(silver_contract.select('ContractNumber','Service_Type'),'ContractNumber','left')\
                                       .join(dealer_workshop, 'Workshop', 'left')
                                           
        #rename columns
        rename_cols = {'Dealer_Name_CN':'ServiceDealer_CN', 
                            'Region': 'ServiceRegion',
                            'Owner_Group':'ServiceGroup'}
        
        for col in rename_cols:
            silver_cost = silver_cost.withColumnRenamed(col, rename_cols[col])
        
        
        return silver_contract,silver_cost
    
        
    
    def _get_silver_esse(self,silver_contract,silver_cost):
        
        """
        Part 2: Silver contract_esse and cost_esse
        """        
        #generate table silver_contract_essence
        silver_contract_essence = silver_contract.select(['ContractNumber',
                                                          'Fin',
                                                          'Baumuster_6',
                                                          'Brand',
                                                          'Class',
                                                          'Model',
                                                          'First_Registration_Date',
                                                          'Signature_Date',
                                                          'Vehicle_Age',
                                                          'Product_EN',
                                                          'Service_Type',
                                                          'Product_Line',
                                                          'Value',
                                                          'GSSN',
                                                          'Dealer_Name_CN',
                                                          'Owner_Group',
                                                          'Region',
                                                          'Province',
                                                          'City'])

        silver_cost = silver_cost.join(silver_contract_essence.select('ContractNumber','FIN','Baumuster_6','Brand',\
                                                             'Class','Model'),'ContractNumber','left')

        create_new_cols = ['Oil','UpperLimit','Diff','Label']     
        
        for new_col in create_new_cols:
            silver_cost = silver_cost.withColumn(new_col,lit('')) 
            
        silver_cost = silver_cost.withColumnRenamed('Service_type','Product_line')
        silver_cost = silver_cost.join(silver_contract_essence.select(['ContractNumber']),'ContractNumber','right')
        silver_cost = silver_cost.filter(silver_cost.ContractNumber.isin(silver_contract_essence.ContractNumber))
        #generate table silver_cost_essence
        silver_cost_essence = silver_cost.select(['ContractNumber',
                                                 'ClaimNo.',
                                                 'Product_EN',
                                                 'Service_type',
                                                 'Damagecode',
                                                 'ClaimAmount',
                                                 'ClaimDate',
                                                 'RepairDate',
                                                 'FirstRegistrationDate',
                                                 'VEGACreditDate',
                                                 'MileageWhenRepair',
                                                 'RepairAge',
                                                 'MileagePerDay',
                                                 'Workshop',
                                                 'ServiceDealer_CN',
                                                 'ServiceGroup',
                                                 'ServiceRegion',
                                                 'Province',
                                                 'City',
                                                 'FIN',
                                                 'Baumuster_6',
                                                 'Brand',
                                                 'Class',
                                                 'Model',
                                                 'Oil',
                                                 'UpperLimit',
                                                 'Diff',
                                                 'Label'])

        
        print("Load data in table tmp_silver_contract_essence")
        silver_contract_essence.registerTempTable("silver_esse")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_silver_contract_essence_info")
        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_silver_contract_essence_info select * from silver_esse")


        return silver_contract_essence,silver_cost_essence


class Prepare_ISP_Info:

    def __init__(self,spark_context):
        self.sc = spark_context
    
    def _get_isp_overview(self,contract_info,cost_info,product_isp_info,\
                           dealer_gssn,dealer_workshop):
        """
        create table blue contract and cost
        """
        #pay attention to brand column      
        isp_contract = contract_info.join(product_isp_info, ['Product','VEGA_Code'], 'inner')
        isp_contract = isp_contract.join(dealer_gssn, 'GSSN', 'left')
        
        rename_cols={"totaldc": "DCCount",'maintb':"B_count","mainta": "A_count","ac": "A/Cfilter_count",
                    "houqiao": "Rear_Bridge_Differential_Oil_count","sp": "SparkPlug_count",
                    "frontbrake": "Front_Brake_Friction_Plate_count","brakeoil": "Brake_Fluid_count",
                    "qianyugua": "Front_Wiper_Blade_count","houyugua": "Rear_Wiper_Blade_count"}        
 
        for col in rename_cols:
            isp_contract = isp_contract.withColumnRenamed(col, rename_cols[col])    


        isp_contract = isp_contract.withColumn('CGI',lit(''))    
        isp_contract = isp_contract.withColumn('OIL',lit(''))
        isp_cost = cost_info.join(product_isp_info, ['Product','VEGA_Code'], 'inner')\
                                  .join(dealer_workshop, 'Workshop', 'left')
        
        isp_cost = isp_cost.join(isp_contract.select(['ContractNumber']),'ContractNumber','right')
        #rename columns                                  
        rename_cols = {'Dealer_Name_CN':'ServiceDealer_CN', 
                            'Region': 'ServiceRegion',
                            'Owner_Group':'ServiceGroup'}
        
        for col in rename_cols:
            isp_cost = isp_cost.withColumnRenamed(col, rename_cols[col])    
            
        isp_blue_cost=isp_cost.filter((isp_cost.Product_line!='Silver')&(isp_cost.Product_line!='Gold')&(isp_cost.Product_line!='Leasing'))
        #use service_type in table contract to cover service_type in table cost
        #merge table dealer_workshop
        isp_silver_cost=isp_cost.filter((isp_cost.Product_line=='Silver')|(isp_cost.Product_line=='Gold')|(isp_cost.Product_line=='Leasing'))

        isp_blue_contract=isp_contract.filter((isp_contract.Product_line!='Silver')&(isp_contract.Product_line!='Gold')&(isp_contract.Product_line!='Leasing'))
        isp_blue_contract=isp_blue_contract.filter((isp_blue_contract.Baumuster_6 != '205343') & (isp_blue_contract.Baumuster_6 != '204052'))           
        isp_silver_contract=isp_contract.filter((isp_contract.Product_line=='Silver')|(isp_contract.Product_line=='Gold')|(isp_contract.Product_line=='Leasing'))                                   
        isp_silver_contract = isp_silver_contract.replace(['1799'],['1800'],'Contract_Duration')        
        isp_contract_icon2 = isp_silver_contract.filter(isp_silver_contract.Source_System =='iCON2')
        isp_contract_no_icon2 = isp_silver_contract.filter(isp_silver_contract.Source_System !='iCON2')
        
        isp_contract_icon2 = isp_contract_icon2.filter((isp_contract_icon2.Contract_Duration == 1440) | \
                       (isp_contract_icon2.Contract_Duration == 1442) | (isp_contract_icon2.Contract_Duration == 360) | \
                       (isp_contract_icon2.Contract_Duration >= 1800) |(isp_contract_icon2.Contract_Duration == 720))
        
        isp_contract_no_icon2 = isp_contract_no_icon2.filter((isp_contract_no_icon2.Contract_Duration == 1440) | \
                       (isp_contract_no_icon2.Contract_Duration == 1442) | (isp_contract_no_icon2.Contract_Duration == 360) | \
                       (isp_contract_no_icon2.Contract_Duration >= 1800))

        isp_silver_contract=isp_contract_icon2.unionAll(isp_contract_no_icon2)    
        
        #generate service_type column and merge table dealer_gssn
        func_get_service_type = udf(get_service_type, StringType())
        isp_silver_contract = isp_silver_contract.withColumn('Service_Type',func_get_service_type('Contract_Duration'))
        
        isp_silver_cost = isp_silver_cost.drop('Service_Type')
        isp_silver_cost = isp_silver_cost.join(isp_silver_contract.select(['ContractNumber','Service_Type']),'ContractNumber','left')        

      
        #utilise this part code that missing dealer id                                     
        #miss_dealer_data=isp_contract.filter(isp_contract.Region.isNull())
        #miss_gssn=list(miss_dealer_data.GSSN_New.unique())
 
#        isp_blue_cost.persist(StorageLevel.MEMORY_AND_DISK_SER)
#        isp_blue_contract.persist(StoragLevel.MEMORY_AND_DISK_SER)  
        isp_blue_cost.persist()
        isp_blue_contract.persist()                           


        return isp_blue_contract,isp_silver_contract, isp_blue_cost ,isp_silver_cost

        
                       
    def _get_isp_esse(self,isp_blue_contract,isp_silver_contract,isp_blue_cost,isp_silver_cost):
        
        """
        Part 1: isp blue contract_esse and isp cost_esse 
        (including blue and blue light product)
        """

        #select columns
        isp_blue_contract_essence = isp_blue_contract.select(['ContractNumber',
                                                             'Fin',
                                                             'Baumuster_6',
                                                             'Brand',
                                                             'Class',
                                                             'Model',
                                                             'First_Registration_Date',
                                                             'Signature_Date',
                                                             'Vehicle_Age',
                                                             'Product_EN',
                                                             'Product',
                                                             'Value',
                                                             'Oil',
                                                             'CGI',
                                                             'DCCount',
                                                             'A_Count',
                                                             'B_Count',
                                                             'A/Cfilter_count',
                                                             'SparkPlug_Count',
                                                             'Rear_Bridge_Differential_Oil_count',
                                                             'Front_Brake_Friction_Plate_count',
                                                             'Brake_Fluid_count',
                                                             'Front_Wiper_Blade_count',
                                                             'Rear_Wiper_Blade_count',
                                                             'GSSN',
                                                             'Dealer_Name_CN',
                                                             'Owner_Group',
                                                             'Region',
                                                             'Province',
                                                             'City',
                                                             'Product_line',
                                                             'Service_Type'])

        isp_blue_cost = isp_blue_cost.join(isp_blue_contract.select(['ContractNumber','Vehicle_Age']),'ContractNumber','left')
        func_label_violate_status = udf(label_violate_status, StringType())
        isp_blue_cost = isp_blue_cost.withColumn('Violate_Status', func_label_violate_status('Vehicle_Age'))

        """
        #change service_type to product_line so that it can append to silver the same as python code
        """
        isp_blue_cost_essence = isp_blue_cost.select(['ContractNumber',
                                              'Claimno',
                                              'Product_EN',
                                              'Product',
                                              'DamageCode',
                                              'Claim_Amount',
                                              'Claim_Date',
                                              'Repair_Date',
                                              'First_Registration_Date',
                                              'Vegacredit_Date',
                                              'Mileage_When_Repair',
                                              'Repair_Age',
                                              'Mileage_Per_Day',
                                              'Workshop',
                                              'ServiceDealer_CN',
                                              'ServiceGroup',
                                              'ServiceRegion',
                                              'Province',
                                              'City',
                                              'Product_line',
                                              'Violate_Status'])
        
        isp_blue_cost_essence = isp_blue_cost_essence.filter(isp_blue_cost_essence.Repair_Age >= 90)            
            
        
        """
        Calculate service times except damagecode:2103601
        """
        # the cost of  '2103601' Damagecode just occupy a little part of total repair fee, as a result, we ignore it
        isp_blue_cost_essence2 = isp_blue_cost_essence.filter(isp_blue_cost_essence.DamageCode != '2103601')
        isp_blue_cost_essence2 = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode.substr(1, 1)!='2')
     
        #calculate out the amount of Damagecode under each ContractNumber,
        isp_blue_count = isp_blue_cost_essence2.groupby('ContractNumber').agg({'DamageCode':'count'})
        
        #after use agg fuction, the result of counting Damagecode column it will generate new column 
        #and it will be 'count(Damagecode)',so we need to rename it
        isp_blue_count = isp_blue_count.withColumnRenamed('count(DamageCode)','ServiceTimes')
        
        #count items  of the damagecode is in '00012SV','00011SV','83108SV','15031SV'
        isp_blue_count_A = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '00012SV')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_A = isp_blue_count_A.withColumnRenamed('count(DamageCode)','A')
        
        isp_blue_count_B = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '00011SV')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_B = isp_blue_count_B.withColumnRenamed('count(DamageCode)','B')
                
        isp_blue_count_ac = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '83108SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_ac = isp_blue_count_ac.withColumnRenamed('count(DamageCode)','A/C')
        
        isp_blue_count_sp = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '15031SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_sp = isp_blue_count_sp.withColumnRenamed('count(DamageCode)','SP')


        isp_blue_count_rbdo = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '3500NSV')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_rbdo = isp_blue_count_rbdo.withColumnRenamed('count(DamageCode)','RBDO')
        
        isp_blue_count_fbfp = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '42114D1')\
                                                .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_fbfp = isp_blue_count_fbfp.withColumnRenamed('count(DamageCode)','FBFP')
        
        
        isp_blue_count_bf = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '43000SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_bf = isp_blue_count_bf.withColumnRenamed('count(DamageCode)','BF')
        
        isp_blue_count_fwb = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '82023SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_fwb = isp_blue_count_fwb.withColumnRenamed('count(DamageCode)','FWB')

        isp_blue_count_rwb = isp_blue_cost_essence2.filter(isp_blue_cost_essence2.DamageCode == '82063SV')\
                                                 .groupby('ContractNumber').agg({'DamageCode':'count'})
        isp_blue_count_rwb = isp_blue_count_rwb.withColumnRenamed('count(DamageCode)','RWB')
        
        #add those 5 new table into blue_count in order to to know the total num and each mount of those four damagecode
        isp_blue_count = isp_blue_count.join(isp_blue_count_A, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_B, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_ac, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_sp, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_rbdo, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_fbfp, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_bf, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_fwb, 'ContractNumber', how = 'left')\
                               .join(isp_blue_count_rwb, 'ContractNumber', how = 'left')
        
        #use 0 to repalce nan value
        isp_blue_count = isp_blue_count.na.fill(0)
                
        """
        determine the contract status
        """
        isp_blue_contract_essence = isp_blue_contract_essence.join(isp_blue_count, 'ContractNumber', how = 'left')
        
        #use 0 to repalce nan value
        isp_fill_na_list = ['ServiceTimes','A','B','A/C','SP','RBDO','FBFP','BF','FWB','RWB']
        isp_fill_na_dict = {i:0 for i in isp_fill_na_list}
        isp_blue_contract_essence = isp_blue_contract_essence.na.fill(isp_fill_na_dict)


        isp_fill_act_na_list = ['DCCount','A_Count','B_Count','A/Cfilter_count','SparkPlug_Count','Rear_Bridge_Differential_Oil_count',\
        'Front_Brake_Friction_Plate_count','Brake_Fluid_count','Front_Wiper_Blade_count','Rear_Wiper_Blade_count']
        isp_fill_act_na_dict = {i:0 for i in isp_fill_act_na_list}
        isp_blue_contract_essence = isp_blue_contract_essence.na.fill(isp_fill_act_na_dict)
                
        """
        generate service detail label column
        """
        func_get_detail_label = udf(get_detail_label, StringType())
        services = {'A':'A_Count','B':'B_Count','A/C':'A/Cfilter_count','SP':'SparkPlug_Count',
        'RBDO':'Rear_Bridge_Differential_Oil_count','FBFP':'Front_Brake_Friction_Plate_count',
        'BF':'Brake_Fluid_count','FWB':'Front_Wiper_Blade_count','RWB':'Rear_Wiper_Blade_count'}
        for i in services:    
            isp_blue_contract_essence = isp_blue_contract_essence.withColumn(i+'_col',lit(i))
            
            isp_blue_contract_essence = isp_blue_contract_essence.withColumn('detail_'+i, \
                                                                     func_get_detail_label(services[i],\
                                                                     i,i+'_col'))

        isp_blue_contract_essence = isp_blue_contract_essence.drop('A_col','B_col','A/C_col','SP_col','RBDO_col','FBFP_col','BF_col','FWB_col','RWB_col')
                    
        """
        generate remainservice column
        """
        func_get_remainservice = udf(get_remainservice,IntegerType())
        isp_blue_contract_essence = isp_blue_contract_essence.withColumn('RemainService', \
                                                                 func_get_remainservice(isp_blue_contract_essence.DCCount,\
                                                                  isp_blue_contract_essence.ServiceTimes))
        
        isp_sert_na_list = ['RemainService']
        isp_sert_na_dict = {i:0 for i in isp_sert_na_list}
        isp_blue_contract_essence = isp_blue_contract_essence.na.fill(isp_sert_na_dict)        
             
        """
        generate status column
        """
        isp_blue_contract_essence = isp_blue_contract_essence.withColumn('Total_Detail',\
                                                                 concat_ws(' ',isp_blue_contract_essence['detail_A'],\
                                                                           isp_blue_contract_essence['detail_B'],\
                                                                           isp_blue_contract_essence['detail_A/C'],\
                                                                           isp_blue_contract_essence['detail_SP'],\
                                                                           isp_blue_contract_essence['detail_RBDO'],\
                                                                           isp_blue_contract_essence['detail_FBFP'],\
                                                                           isp_blue_contract_essence['detail_BF'],\
                                                                           isp_blue_contract_essence['detail_FWB'],\
                                                                           isp_blue_contract_essence['detail_RWB']))

        isp_blue_max_mile = isp_blue_cost_essence.groupby('ContractNumber').agg({'Mileage_When_Repair':'max'}).withColumnRenamed('max(Mileage_When_Repair)','Max_Mileage')
        isp_blue_contract_essence=isp_blue_contract_essence.join(isp_blue_max_mile,'ContractNumber','left')

        isp_blue_max_rdate=isp_blue_cost_essence.groupby('ContractNumber').agg({'Repair_Date':'max'}).withColumnRenamed('max(Repair_Date)','Max_Repair_Date')
        isp_blue_contract_essence=isp_blue_contract_essence.join(isp_blue_max_rdate,'ContractNumber','left')

        func_get_isp_blue_status = udf(lable_isp_blue_status, StringType())
        isp_blue_contract_essence = isp_blue_contract_essence.withColumn('Status', \
                                                                 func_get_isp_blue_status('Product_line','Signature_Date','Max_Repair_Date','Max_Mileage','RemainService','ServiceTimes'))
        
        isp_blue_contract_essence = isp_blue_contract_essence.drop('Max_Mileage','Max_RepairDate')


        """
        calculate cost by contract/claim
        """
        isp_blue_costbycontract = isp_blue_cost_essence.groupby('ContractNumber')\
                                                       .agg({'Claim_Amount':'sum'})
        isp_blue_costbycontract = isp_blue_costbycontract.withColumnRenamed('Sum(Claim_Amount)','CostTillNow')
        
        #merge back to contract table        
        isp_blue_contract_essence = isp_blue_contract_essence.join(isp_blue_costbycontract, 'ContractNumber', 'left')
        #repalce na with 0 in costTillNow column
        isp_blue_contract_essence = isp_blue_contract_essence.na.fill({'CostTillNow': 0})
        
        
        """
        generate balance column
        """
        isp_blue_contract_essence = isp_blue_contract_essence.withColumn('Balance',\
                                                                 (isp_blue_contract_essence.Value - \
                                                                  isp_blue_contract_essence.CostTillNow))


        isp_silver_max_rdate=isp_silver_cost.groupby('ContractNumber').agg({'Repair_Date':'max'}).withColumnRenamed('max(Repair_Date)','Max_Repair_Date')
        isp_silver_contract=isp_silver_contract.join(isp_silver_max_rdate,'ContractNumber','left')

        func_get_isp_silver_status = udf(lable_silver_status, StringType())
        isp_silver_contract = isp_silver_contract.withColumn('Status',func_get_isp_silver_status('Planned_Contract_End','Max_Repair_Date'))
        isp_silver_contract_essence = isp_silver_contract.select(['ContractNumber',
                                                                     'Fin',
                                                                     'Baumuster_6',
                                                                     'Brand',
                                                                     'Class',
                                                                     'Model',
                                                                     'Vega_Code',
                                                                     'Product',
                                                                     'First_Registration_Date',
                                                                     'Signature_Date',
                                                                     'Vehicle_Age',
                                                                     'Status',
                                                                     'Product_EN',
                                                                     'Service_type',
                                                                     'Product_line',
                                                                     'Value',
                                                                     'GSSN',
                                                                     'Dealer_Name_CN',
                                                                     'Owner_Group',
                                                                     'Region',
                                                                     'Province',
                                                                     'City'])
 
        isp_silver_cost = isp_silver_cost.join(isp_silver_contract_essence.select('ContractNumber','Fin','Baumuster_6','Brand',\
                                                             'Class','Model'),'ContractNumber','left')

        create_new_cols = ['Oil','UpperLimit','Diff','Label']             
        for new_col in create_new_cols:
            isp_silver_cost = isp_silver_cost.withColumn(new_col,lit('')) 
       
        """
        #filter out contract that is not silver
        """
        isp_silver_cost = isp_silver_cost.join(isp_silver_contract_essence.select(['ContractNumber']),'ContractNumber','inner')     
        isp_silver_cost_essence = isp_silver_cost.select(['ContractNumber',
                                                         'Claimno',
                                                         'Product_EN',
                                                         'Service_type',
                                                         'DamageCode',
                                                         'Claim_Amount',
                                                         'Claim_Date',
                                                         'Repair_Date',
                                                         'First_Registration_Date',
                                                         'VegaCredit_Date',
                                                         'Mileage_When_Repair',
                                                         'Repair_Age',
                                                         'Mileage_Per_Day',
                                                         'Workshop',
                                                         'ServiceDealer_CN',
                                                         'ServiceGroup',
                                                         'ServiceRegion',
                                                         'Province',
                                                         'City'])       

        isp_silver_cost_essence = isp_silver_cost_essence.withColumnRenamed('Service_type','Product_line')  

        isp_blue_cost_essence.persist()
        isp_blue_contract_essence.persist()
        isp_silver_cost_essence.persist()
        isp_silver_contract_essence.persist()
    
#        print("Load data in table tmp_blue_cost_essence_info")
#        isp_blue_cost_essence.registerTempTable("blue_cost_esse")
#        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_isp_blue_cost_essence_info")
#        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_isp_blue_cost_essence_info select * from isp_blue_cost_esse")       
#                
        
        return isp_blue_contract_essence,isp_blue_cost_essence,isp_silver_contract_essence,isp_silver_cost_essence
    
    
    #
    def _get_isp_blue_detect(self,isp_blue_cost):
        
        isp_blue_cost_detect = isp_blue_cost.select(['ContractNumber','Claimno','Repair_Date','DamageCode'])
        isp_blue_cost_detect = isp_blue_cost_detect.filter((isp_blue_cost_detect.DamageCode == '00012SV') | \
                                                   (isp_blue_cost_detect.DamageCode == '00011SV'))
        
        
        #create sortorder column
        isp_blue_cost_detect_pd = isp_blue_cost_detect.toPandas()
        isp_blue_cost_detect_pd['Sortorder'] = isp_blue_cost_detect_pd.groupby('ContractNumber')['Repair_Date'].rank(ascending=0,method = 'dense')
        
        isp_blue_cost_detect_pd['LaggedDate'] = isp_blue_cost_detect_pd.groupby(['ContractNumber'])['Repair_Date'].shift(1)
        isp_blue_cost_detect_pd['LaggedDC'] = isp_blue_cost_detect_pd.groupby(['ContractNumber'])['DamageCode'].shift(1)
        isp_blue_cost_detect_pd['LaggedSortOrder'] = isp_blue_cost_detect_pd.groupby(['ContractNumber'])['Sortorder'].shift(1)
        isp_blue_cost_detect_pd = isp_blue_cost_detect_pd[-isp_blue_cost_detect_pd.LaggedDate.isnull()]
        isp_blue_cost_detect_pd = isp_blue_cost_detect_pd[isp_blue_cost_detect_pd.Sortorder <= isp_blue_cost_detect_pd.LaggedSortOrder]
        isp_blue_cost_detect_pd['DayGap'] = isp_blue_cost_detect_pd['Repair_Date'] - isp_blue_cost_detect_pd['LaggedDate'] 
        isp_blue_cost_detect_pd['DayGap'] = isp_blue_cost_detect_pd['DayGap'].apply(lambda x: x/np.timedelta64(1,'D'))
        isp_blue_cost_detect_pd = isp_blue_cost_detect_pd[isp_blue_cost_detect_pd.DayGap < 10]

        #in testing, please verify the column name of claimno
        #and check the data type of column related to date
        schema = StructType([StructField("ContractNumber", StringType(), True)
                             ,StructField("Claimno", StringType(), True)
                             ,StructField("RepairDate", DateType(), True)
                             ,StructField("DamageCode", StringType(), True)
                             ,StructField("SortOrder", FloatType(), True)
                             ,StructField("LaggedDate", DateType(), True)
                             ,StructField("LaggedDC", StringType(), True)
                             ,StructField("LaggedSortOrder", FloatType(), True)
                             ,StructField("DayGap", FloatType(), True)])


        isp_blue_cost_detect = spark.createDataFrame(isp_blue_cost_detect_pd, schema)
        
        print("Load data in table tmp_blue_cost_detect_info")
        isp_blue_cost_detect.registerTempTable("isp_blue_cost_detect")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_isp_blue_cost_detect_info")
        self.sc.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_isp_blue_cost_detect_info select * from isp_blue_cost_detect")       
        
        return isp_blue_cost_detect
        


#change contract_sales null to 0
def set_null_zero(contractsales):
    if contractsales:
        contractsales = contractsales
    else:
        contractsales = 0
    
#calculate which week in a year
def change_name(Brand):
    
    if Brand=='MB':
        Brand = 'Mercedes-Benz'
    if Brand == 'smart':
        Brand = 'Smart'
        
    return Brand



#def get_penetrationrate(Contract_Sales,Vehicle_Sales):
#    if Contract_Sales:
#        if Vehicle_Sales and Vehicle_Sales !=0:
#            Penetration_Rate = round((Contract_Sales*1.0/Vehicle_Sales),4)
#        else:
#            Penetration_Rate = 0.0            
#    else:
#        Penetration_Rate = 0.0
#    
#    return Penetration_Rate


def get_month_numb(signaturedate):
    if isinstance(signaturedate,str):        
        month_numb = signaturedate[0:6]
    else:
        signaturedate = datetime.strftime(signaturedate,'%Y-%m-%d').replace('-','')
        month_numb = signaturedate[0:6]    
    return month_numb


#we needn`t to calculate the data before the date 2018
def get_week(signaturedate,statartdate,enddate,week):
    signaturedate = datetime.strftime(signaturedate,'%Y-%m-%d').replace('-','')
    if signaturedate and statartdate and enddate and week:
        if  (int(signaturedate) >= int(statartdate)) & (int(signaturedate) <= int(enddate)):
                week_cal = week
                
        else:
                week_cal = ''
    else:
             week_cal = ''
    return week_cal


def get_year(End_Date):
    Year = End_Date[0:4]
    return Year
       
def get_month(End_Date):
    Month = End_Date[5:6]
    return Month

def correct_region(region):
    if region:
        if region.startswith('S') or region.startswith('s') :
            region = 'South'
        if region.startswith('N') or region.startswith('n'):
            region = 'North'    
        if region.startswith('E') or region.startswith('e'):
            region = 'East'
        if region.startswith('W') or region.startswith('w'):
            region = 'West'
    else:
        region = region
    return region


def change_date(int_date):
    changed_date = datetime.strptime(str(int_date),'%Y%m%d')
    return changed_date


def drop_over_month(year,month,max_year,max_month):
    if year == max_year:
        if month <= max_month:
            month_type = 1
        else:
            month_type = 0
    else:
        month_type = 1
    return month_type    

class Prepare_PenetrationRate:
    
    def __init__(self,year_string_list,dealersales_monthly,dealer_gssn,spark_context):
        self.ysl = year_string_list
        self.dsm = dealersales_monthly
        self.dg = dealer_gssn
        self.sc = spark_context
        
    
    def _get_total_sale(self,blue_contract,silver_contract):
        
        
        blue_sale = blue_contract.select(['ContractNumber',
                                          'Signature_Date',
                                          'Fin',
                                          'Baumuster_6',
                                          'Brand',
                                          'Class',
                                          'Model',
                                          'Product_EN',
                                          'Service_Type',
                                          'GSSN',
                                          'Region',
                                          'Value'])
            
        blue_sale = blue_sale.withColumnRenamed('Service_Type','Product')    
        

        silver_sale = silver_contract.select(['ContractNumber',
                                              'Signature_Date',
                                              'Fin',
                                              'Baumuster_6',
                                              'Brand',
                                              'Class',
                                              'Model',
                                              'Product_EN',
                                              'Service_Type',
                                              'GSSN',
                                              'Region',
                                              'Value'])
        
        silver_sale = silver_sale.withColumnRenamed('Service_Type','Product')    
            
    
        total_sale = blue_sale.unionAll(silver_sale)
    
        total_sale = total_sale.withColumn('Year', year(total_sale.Signature_Date))
        total_sale = total_sale.withColumn('Month', month(total_sale.Signature_Date))


#       Region exists wrong spelling： SOUTH should be South
        func_correct_region = udf(correct_region, StringType())
        total_sale = total_sale.withColumn('Region',func_correct_region(total_sale.Region))
                
        return total_sale

    def _get_isp_total_sale(self,isp_blue_contract,isp_silver_contract):
        isp_blue_sale = isp_blue_contract.select(['ContractNumber',
                                          'Signature_Date',
                                          'Fin',
                                          'Baumuster_6',
                                          'Brand',
                                          'Class',
                                          'Model',
                                          'Product_EN',
                                          'Product_line',
                                          'GSSN',
                                          'Region',
                                          'Value'])
            
        isp_blue_sale = isp_blue_sale.withColumnRenamed('Product_line','Product')    
        isp_silver_sale = isp_silver_contract.select(['ContractNumber',
                                              'Signature_Date',
                                              'Fin',
                                              'Baumuster_6',
                                              'Brand',
                                              'Class',
                                              'Model',
                                              'Product_EN',
                                              'Service_Type',
                                              'GSSN',
                                              'Region',
                                              'Value'])
        
        isp_silver_sale = isp_silver_sale.withColumnRenamed('Service_Type','Product')    
                
        isp_total_sale = isp_blue_sale.unionAll(isp_silver_sale)    
        isp_total_sale = isp_total_sale.withColumn('Year', year(isp_total_sale.Signature_Date))
        isp_total_sale = isp_total_sale.withColumn('Month', month(isp_total_sale.Signature_Date))
        func_correct_region = udf(correct_region, StringType())
        isp_total_sale = isp_total_sale.withColumn('Region',func_correct_region(isp_total_sale.Region))
                
        return isp_total_sale


        
    def _get_pr_monthly(self,blue_contract,silver_contract):
        
        total_sale = self._get_total_sale(blue_contract,silver_contract)
        
        total_sale.cache()
        total_sale = total_sale.withColumnRenamed('Product','Product_Line')
        
        total_sale_monthly = total_sale.groupby(['Product_en','GSSN',\
                                                 'Year','Month',\
                                                 'Product_Line',\
                                                 'Brand',\
                                                 'Class',\
                                                 'Model',\
                                                 'Baumuster_6']).agg({'ContractNumber':'count'})
    
        rename_cols = {'count(ContractNumber)':'SalesVolume','Product_EN':'Product'}
        for i in rename_cols:
            total_sale_monthly = total_sale_monthly.withColumnRenamed(i,rename_cols[i])
                 
#        dsm_cp = dealersales_monthly     
        dsm_cp = self.dsm
        
        func_change_name = udf(change_name, StringType())        
        dsm_cp = dsm_cp.withColumn('Brand',func_change_name('Brand'))
                
        total_pr_monthly = total_sale_monthly.join(dsm_cp,\
                                                 ['GSSN','Year','Month','Brand'],'outer')
        

        #we need to fill nan in region with dealer_gssn """
        dg_cp = self.dg
#        dg_cp = dealer_gssn
        total_pr_monthly_null = total_pr_monthly.filter(total_pr_monthly.Region.isNull())
        total_pr_monthly_not_null = total_pr_monthly.filter(total_pr_monthly.Region.isNotNull())
        
        total_pr_monthly_null = total_pr_monthly_null.drop('Dealer_Name_CN').drop('Region').drop('Owner_Group').drop('Province').drop('City')     
        total_pr_monthly_null = total_pr_monthly_null.join(dg_cp.select(['GSSN','Dealer_Name_CN','Region','Owner_Group','Province','City']), ['GSSN'],'left')
        
        total_pr_monthly = total_pr_monthly_not_null.union(total_pr_monthly_null)
        
        #max month in latest year       
        max_year = dsm_cp.groupBy().agg({'Year':'max'}).toPandas()['max(Year)'][0]
        max_month = dsm_cp.filter(dsm_cp.Year == int(max_year)).groupBy().agg({'Month':'max'}).toPandas()['max(Month)'][0]
                
        total_pr_monthly = total_pr_monthly.withColumn('max_year',lit(int(max_year)))
        total_pr_monthly = total_pr_monthly.withColumn('max_month',lit(int(max_month))) 
        
        func_drop_over_month = udf(drop_over_month, IntegerType())
        total_pr_monthly = total_pr_monthly.withColumn('Month_Type',\
                                                       func_drop_over_month(total_pr_monthly.Year,\
                                                       total_pr_monthly.Month,\
                                                       total_pr_monthly.max_year,\
                                                       total_pr_monthly.max_month))
                                                       
        total_pr_monthly = total_pr_monthly.filter(total_pr_monthly.Month_Type == 1).drop('Month_Type','max_month','max_year')
        
        total_pr_monthly = total_pr_monthly.select(['GSSN',
                                                  'SAP', 
                                                  'Year',
                                                  'Month',
                                                  'Dealer_Name_CN',
                                                  'Owner_Group', 
                                                  'Region', 
                                                  'Province',
                                                  'City',
                                                  'Product',
                                                  'Product_Line',
                                                  'Baumuster_6',
                                                  'Brand',
                                                  'Class',
                                                  'Model',
                                                  'SalesVolume', 
                                                  'Sales_Volumn'])
    
        rename_cols = {'SalesVolume':'ContractSales','Sales_Volumn':'VehicleSales'}
        
        for i in rename_cols:
        
        total_pr_monthly = total_pr_monthly.na.fill({'ContractSales':0})
        total_pr_monthly = total_pr_monthly.withColumn('PenetrationRate',\
                                             (total_pr_monthly.ContractSales/total_pr_monthly.VehicleSales))
        total_pr_monthly.registerTempTable("monthly_inf")
        spark.sql("truncate table revr_bmbs_srv_contract_blue.tgt_pr_monthly_info")                             
        spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_pr_monthly_info select * from monthly_inf")
                                         
        
        return total_pr_monthly 


    def _get_isp_pr_monthly(self,isp_blue_contract,isp_silver_contract):
        
        isp_total_sale = self._get_isp_total_sale(isp_blue_contract,isp_silver_contract)
        
        isp_total_sale.persist()
        isp_total_sale = isp_total_sale.withColumnRenamed('Product','Product_Line')
        
        isp_total_sale_monthly = isp_total_sale.groupby(['Product_en','GSSN',\
                                                 'Year','Month',\
                                                 'Product_Line',\
                                                 'Brand',\
                                                 'Class',\
                                                 'Model',\
                                                 'Baumuster_6']).agg({'ContractNumber':'count'})
    
        rename_cols = {'count(ContractNumber)':'SalesVolume','Product_EN':'Product'}
        for i in rename_cols:
            isp_total_sale_monthly = isp_total_sale_monthly.withColumnRenamed(i,rename_cols[i])
                 
#        dsm_cp = dealersales_monthly     
        dsm_cp = self.dsm
        
        func_change_name = udf(change_name, StringType())        
        dsm_cp = dsm_cp.withColumn('Brand',func_change_name('Brand'))
                
        isp_total_pr_monthly = isp_total_sale_monthly.join(dsm_cp,\
                                                 ['GSSN','Year','Month','Brand'],'outer')
        

        #we need to fill nan in region with dealer_gssn """
        dg_cp = self.dg
        #dg_cp = dealer_gssn
        isp_total_pr_monthly_null = isp_total_pr_monthly.filter(isp_total_pr_monthly.Region.isNull())
        isp_total_pr_monthly_not_null = isp_total_pr_monthly.filter(isp_total_pr_monthly.Region.isNotNull())
        
        isp_total_pr_monthly_null = isp_total_pr_monthly_null.drop('Dealer_Name_CN').drop('Region').drop('Owner_Group').drop('Province').drop('City')     
        isp_total_pr_monthly_null = isp_total_pr_monthly_null.join(dg_cp.select(['GSSN','Dealer_Name_CN','Region','Owner_Group','Province','City']), ['GSSN'],'left')
        
        isp_total_pr_monthly = isp_total_pr_monthly_not_null.union(isp_total_pr_monthly_null)
        
        #max month in latest year       
        max_year = dsm_cp.groupBy().agg({'Year':'max'}).toPandas()['max(Year)'][0]
        max_month = dsm_cp.filter(dsm_cp.Year == int(max_year)).groupBy().agg({'Month':'max'}).toPandas()['max(Month)'][0]
                
        isp_total_pr_monthly = isp_total_pr_monthly.withColumn('max_year',lit(int(max_year)))
        isp_total_pr_monthly = isp_total_pr_monthly.withColumn('max_month',lit(int(max_month))) 
        
        func_drop_over_month = udf(drop_over_month, IntegerType())
        isp_total_pr_monthly = isp_total_pr_monthly.withColumn('Month_Type',\
                                                       func_drop_over_month(isp_total_pr_monthly.Year,\
                                                       isp_total_pr_monthly.Month,\
                                                       isp_total_pr_monthly.max_year,\
                                                       isp_total_pr_monthly.max_month))
                                                       
        isp_total_pr_monthly = isp_total_pr_monthly.filter(isp_total_pr_monthly.Month_Type == 1).drop('Month_Type','max_month','max_year')
        
        isp_total_pr_monthly = isp_total_pr_monthly.select(['GSSN',
                                                  'SAP', 
                                                  'Year',
                                                  'Month',
                                                  'Dealer_Name_CN',
                                                  'Owner_Group', 
                                                  'Region', 
                                                  'Province',
                                                  'City',
                                                  'Product',
                                                  'Product_Line',
                                                  'Baumuster_6',
                                                  'Brand',
                                                  'Class',
                                                  'Model',
                                                  'SalesVolume', 
                                                  'Sales_Volumn'])
    
        rename_cols = {'SalesVolume':'ContractSales','Sales_Volumn':'VehicleSales'}
        
        for i in rename_cols:
            isp_total_pr_monthly = isp_total_pr_monthly.withColumnRenamed(i,rename_cols[i])

        
        isp_total_pr_monthly = isp_total_pr_monthly.na.fill({'ContractSales':0})
        isp_total_pr_monthly = isp_total_pr_monthly.withColumn('PenetrationRate',\
                                             (isp_total_pr_monthly.ContractSales/isp_total_pr_monthly.VehicleSales))
        isp_total_pr_monthly.registerTempTable("isp_monthly_inf")
        spark.sql("truncate table revr_bmbs_srv_contract_blue.tgt_isp_pr_monthly_info")                             
        spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_isp_pr_monthly_info select * from isp_monthly_inf")
                                         
        
        return isp_total_pr_monthly 


    def _get_pr_weekly(self,blue_contract,silver_contract):
        
        total_sale = self._get_total_sale(blue_contract,silver_contract)
        #they do not provide the data before 2018
        total_sale = total_sale.filter(total_sale.Year >= '2018').withColumnRenamed('Product','Product_Line')
        
        total_sale.cache()

        #generate new column month_numb to prepare for joining table week，for example: '2018-03-01' -> '201803'
        cesar_week = spark.sql("""select case when brand == 'MB' then 'Mercedes-Benz' 
               else 'Smart' end as brand,
               region,wtd,mtd,ytd,week,cast(start_date as string) start_date,
               cast(end_date as string) end_date from 
               revr_bmbs_srv_contract_blue.src_cesar_week_info """)
       
        print("Rename column")
        schema = StructType([StructField("Brand", StringType(), True)
                            ,StructField("Region", StringType(), True)
                            ,StructField("WTD", IntegerType(), True)
                            ,StructField("MTD",IntegerType(),True)
                            ,StructField("YTD", IntegerType(), True)
                            ,StructField("Week", IntegerType(), True)
                            ,StructField("Start_Date", StringType(), True)
                            ,StructField("End_Date", StringType(), True)])
            
        cesar_week = spark.createDataFrame(cesar_week.rdd, schema)

        func_get_year = udf(get_year,StringType())
        cesar_week = cesar_week.withColumn('Year',func_get_year('End_Date'))
        
        func_get_month = udf(get_month,StringType())
        cesar_week = cesar_week.withColumn('Month',func_get_month('End_Date'))
       
        col_names = {'WTD':'WTD_newcar','MTD':'MTD_newcar','YTD':'YTD_newcar'} 
        for col_name in col_names:
            cesar_week = cesar_week.withColumnRenamed(col_name,col_names[col_name])     
                        
        week_list = cesar_week.drop_duplicates(['Week']).select('Week').toPandas()
        week_list['Week'] = week_list['Week'].astype(str)            

        for num in range(len(week_list)):
            tmp_table = cesar_week.filter(cesar_week.Week == week_list.Week[num])
            start_date = tmp_table.drop_duplicates(['Start_Date']).select('Start_Date').toPandas().Start_Date[0]
            end_date = tmp_table.drop_duplicates(['End_Date']).select('End_Date').toPandas().End_Date[0]
            diff = int(end_date) - int(start_date)
            tmp_start_date = start_date
            if num == 0:
                for i in range(diff+1):
                    if i == 0:
                        j=0
                        final = tmp_table.withColumn('Mapping_Date',lit(int(start_date)+j))
                        
                    else:
                        j=1
                        tmp_start_date = int(tmp_start_date) + j 
                        tmp = tmp_table.withColumn('Mapping_Date',lit(int(tmp_start_date)))
                        final = final.unionAll(tmp)
            else:
                j=0
                for x in range(diff+1):
                    tmp_start_date = int(tmp_start_date) + j
                    tmp_table = tmp_table.withColumn('Mapping_Date',lit(int(tmp_start_date)))
                    j=1
                    final = final.unionAll(tmp_table)
                        
        cesar_week = final
        
        print("change date")
        func_change_date = udf(change_date,DateType())
        cesar_week = cesar_week.withColumn('Mapping_Date', func_change_date('Mapping_Date'))

        cesar_week = cesar_week.withColumnRenamed('Region','Region_cw')
        cesar_week = cesar_week.withColumnRenamed('Brand','Brand_cw')

        cond = [total_sale.Region == cesar_week.Region_cw, total_sale.Brand == cesar_week.Brand_cw, total_sale.Signature_Date == cesar_week.Mapping_Date]
        total_sale_weekly = total_sale.join(cesar_week.select(['Brand_cw','Region_cw',\
        'WTD_newcar','MTD_newcar','YTD_newcar','Week','Start_Date','End_Date','Mapping_Date']),cond, how = 'left')
         
        total_sale_weekly.cache()
             
        total_sale_weekly_ym = total_sale_weekly.groupby(['Product_EN','Product_Line',\
                                                 'Year',\
                                                 'Month',\
                                                 'Week',\
                                                 'Brand',\
                                                 'Region']).agg({'ContractNumber':'count','Value':'sum'})
    
    
        rename_cols = {'count(contractnumber)':'SalesVolume','sum(Value)':'Revenue'}
        for i in rename_cols:
            total_sale_weekly_ym = total_sale_weekly_ym.withColumnRenamed(i,rename_cols[i])
                
        total_sale_weekly_YTD = total_sale_weekly_ym.groupby(['Year',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','YTD_contractsale')\
            .withColumnRenamed('sum(Revenue)','YTD_Revenue')
          
        total_sale_weekly_MTD = total_sale_weekly_ym.groupby(['Year','Month',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','MTD_contractsale')\
            .withColumnRenamed('sum(Revenue)','MTD_Revenue')
            
        total_sale_weekly_WTD = total_sale_weekly_ym.groupby(['Year','Month','Week',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','WTD_contractsale').withColumnRenamed('sum(Revenue)','WTD_Revenue')
        
        
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('MTD_contractsale',lit(0))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('MTD_Revenue',lit(0))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('WTD_contractsale',lit(0))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('WTD_Revenue',lit(0))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('Month',lit(''))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('Week',lit(''))
        total_sale_weekly_YTD = total_sale_weekly_YTD.withColumn('DataType',lit('YTD'))
        

        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('YTD_contractsale',lit(0))
        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('YTD_Revenue',lit(0))            
        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('WTD_contractsale',lit(0))
        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('WTD_Revenue',lit(0))
        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('Week',lit(''))
        total_sale_weekly_MTD = total_sale_weekly_MTD.withColumn('DataType',lit('MTD'))

        
        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('YTD_contractsale',lit(0))
        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('YTD_Revenue',lit(0))          
        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('MTD_contractsale',lit(0))
        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('MTD_Revenue',lit(0))

        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('Week',total_sale_weekly_WTD.Week.cast('String'))
        total_sale_weekly_WTD = total_sale_weekly_WTD.withColumn('DataType',lit('WTD'))
        

        total_sale_weekly_YTD = total_sale_weekly_YTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])        
        total_sale_weekly_MTD = total_sale_weekly_MTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        total_sale_weekly_WTD = total_sale_weekly_WTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
            
        
        #total is not added up by the four region volume in cesar_week
        print("generate total table") 
        completed_sale_YTD = total_sale_weekly_YTD.groupby(['Year','Brand','Product_EN','Product_Line'])\
                                               .agg({'YTD_contractsale':'sum','YTD_Revenue':'sum'})

        completed_sale_MTD = total_sale_weekly_MTD.groupby(['Year','Month','Brand','Product_EN','Product_Line'])\
                                               .agg({'MTD_contractsale':'sum','MTD_Revenue':'sum'})
            
        completed_sale_WTD = total_sale_weekly_WTD.groupby(['Year','Month','Week','Brand','Product_EN','Product_Line'])\
                                               .agg({'WTD_contractsale':'sum','WTD_Revenue':'sum'})
#        
        completed_sale_YTD = completed_sale_YTD.withColumn('Region',lit('Total'))
        completed_sale_MTD = completed_sale_MTD.withColumn('Region',lit('Total'))
        completed_sale_WTD = completed_sale_WTD.withColumn('Region',lit('Total'))
        
        completed_sale_YTD = completed_sale_YTD.withColumn('Month',lit(''))
        completed_sale_YTD = completed_sale_YTD.withColumn('Week',lit(''))
        completed_sale_YTD = completed_sale_YTD.withColumn('MTD_contractsale',lit(0))
        completed_sale_YTD = completed_sale_YTD.withColumn('MTD_Revenue',lit(0))
        completed_sale_YTD = completed_sale_YTD.withColumn('WTD_contractsale',lit(0))
        completed_sale_YTD = completed_sale_YTD.withColumn('WTD_Revenue',lit(0))
        completed_sale_YTD = completed_sale_YTD.withColumn('DataType',lit('Total_ytd'))


        completed_sale_MTD = completed_sale_MTD.withColumn('Week',lit(''))
        completed_sale_MTD = completed_sale_MTD.withColumn('YTD_contractsale',lit(0))
        completed_sale_MTD = completed_sale_MTD.withColumn('YTD_Revenue',lit(0))
        completed_sale_MTD = completed_sale_MTD.withColumn('WTD_contractsale',lit(0))
        completed_sale_MTD = completed_sale_MTD.withColumn('WTD_Revenue',lit(0))
        completed_sale_MTD = completed_sale_MTD.withColumn('DataType',lit('Total_mtd'))


        completed_sale_WTD = completed_sale_WTD.withColumn('YTD_contractsale',lit(0))
        completed_sale_WTD = completed_sale_WTD.withColumn('YTD_Revenue',lit(0))
        completed_sale_WTD = completed_sale_WTD.withColumn('MTD_contractsale',lit(0))
        completed_sale_WTD = completed_sale_WTD.withColumn('MTD_Revenue',lit(0))
        completed_sale_WTD = completed_sale_WTD.withColumn('DataType',lit('Total_wtd'))

#        
        completed_sale_YTD = completed_sale_YTD.withColumnRenamed('sum(YTD_contractsale)','YTD_contractsale')
        completed_sale_YTD = completed_sale_YTD.withColumnRenamed('sum(YTD_Revenue)','YTD_Revenue')
        completed_sale_MTD = completed_sale_MTD.withColumnRenamed('sum(MTD_contractsale)','MTD_contractsale')
        completed_sale_MTD = completed_sale_MTD.withColumnRenamed('sum(MTD_Revenue)','MTD_Revenue')
        completed_sale_WTD = completed_sale_WTD.withColumnRenamed('sum(WTD_contractsale)','WTD_contractsale')
        completed_sale_WTD = completed_sale_WTD.withColumnRenamed('sum(WTD_Revenue)','WTD_Revenue')
        
        completed_sale_YTD = completed_sale_YTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        completed_sale_MTD = completed_sale_MTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        completed_sale_WTD = completed_sale_WTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])

        
        total_sale_YTD = total_sale_weekly_YTD.unionAll(completed_sale_YTD)
        total_sale_MTD = total_sale_weekly_MTD.unionAll(completed_sale_MTD)
        total_sale_WTD = total_sale_weekly_WTD.unionAll(completed_sale_WTD)


        print("generate sale_pr_weekly")
        cesar_week = spark.sql("""select case when brand == 'MB' then 'Mercedes-Benz' 
               else 'Smart' end as brand,
               region,wtd,mtd,ytd,week,cast(start_date as string) start_date,
               cast(end_date as string) end_date from 
               revr_bmbs_srv_contract_blue.src_cesar_week_info """)
       
        print("Rename column")
        schema = StructType([StructField("Brand", StringType(), True)
                            ,StructField("Region", StringType(), True)
                            ,StructField("WTD", IntegerType(), True)
                            ,StructField("MTD",IntegerType(),True)
                            ,StructField("YTD", IntegerType(), True)
                            ,StructField("Week", IntegerType(), True)
                            ,StructField("Start_Date", StringType(), True)
                            ,StructField("End_Date", StringType(), True)])
            
        cesar_week = spark.createDataFrame(cesar_week.rdd, schema)
        
        func_get_year = udf(get_year,StringType())
        cesar_week = cesar_week.withColumn('Year',func_get_year('End_Date'))
        
        func_get_month = udf(get_month,StringType())
        cesar_week = cesar_week.withColumn('Month',func_get_month('End_Date'))
       
       
        col_names = {'WTD':'WTD_newcar','MTD':'MTD_newcar','YTD':'YTD_newcar'} 
        for col_name in col_names:
            cesar_week = cesar_week.withColumnRenamed(col_name,col_names[col_name])     
                      
        cesar_week = cesar_week.withColumnRenamed('Year','Year_cw')
        cesar_week = cesar_week.withColumnRenamed('Month','Month_cw')
        cesar_week = cesar_week.withColumnRenamed('Week','Week_cw')
        cesar_week = cesar_week.withColumnRenamed('Region','Region_cw')
        cesar_week = cesar_week.withColumnRenamed('Brand','Brand_cw')

        max_month = cesar_week.groupby().agg({'Month_cw':'max'}).toPandas()['max(Month_cw)'][0]
        max_week = cesar_week.groupby().agg({'Week_cw':'max'}).toPandas()['max(Week_cw)'][0]
        
        cesar_week = cesar_week.filter(cesar_week.Week_cw == str(max_week))        

        cond = [total_sale_YTD.Region == cesar_week.Region_cw, total_sale_YTD.Brand == cesar_week.Brand_cw, total_sale_YTD.Year == cesar_week.Year_cw]
        sale_pr_weekly_YTD = total_sale_YTD.join(cesar_week.select(['Region_cw','Brand_cw','Year_cw','YTD_newcar']),cond,'left')

        cond = [total_sale_MTD.Region == cesar_week.Region_cw, total_sale_MTD.Brand == cesar_week.Brand_cw,\
                total_sale_MTD.Year == cesar_week.Year_cw,total_sale_MTD.Month==cesar_week.Month_cw]
        sale_pr_weekly_MTD = total_sale_MTD.join(cesar_week.select(['Region_cw','Brand_cw','Year_cw','Month_cw','MTD_newcar']),cond,'left')    

        cond = [total_sale_WTD.Region == cesar_week.Region_cw, total_sale_WTD.Brand == cesar_week.Brand_cw, \
                total_sale_WTD.Year == cesar_week.Year_cw,total_sale_WTD.Month==cesar_week.Month_cw,total_sale_WTD.Week == cesar_week.Week_cw]
        sale_pr_weekly_WTD = total_sale_WTD.join(cesar_week.select(['Region_cw',\
        'Brand_cw','Year_cw','Month_cw','Week_cw','WTD_newcar','Start_Date','End_Date']),cond,'left')

        
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('YTD_pr',round((sale_pr_weekly_YTD['YTD_contractsale']/sale_pr_weekly_YTD['YTD_newcar']),4))
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('MTD_pr',round((sale_pr_weekly_MTD['MTD_contractsale']/sale_pr_weekly_MTD['MTD_newcar']),4))
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.withColumn('WTD_pr',round((sale_pr_weekly_WTD['WTD_contractsale']/sale_pr_weekly_WTD['WTD_newcar']),4))

        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('MTD_newcar',lit(0))  
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('WTD_newcar',lit(0))  
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('MTD_pr',lit(0.0))  
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('WTD_pr',lit(0.0))  
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('Start_Date',lit(''))  
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('End_Date',lit(''))  

        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('YTD_newcar',lit(0))  
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('WTD_newcar',lit(0))  
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('YTD_pr',lit(0.0))  
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('WTD_pr',lit(0.0))  
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('Start_Date',lit(''))  
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('End_Date',lit(''))         
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.drop('Month_cw')


        sale_pr_weekly_WTD = sale_pr_weekly_WTD.withColumn('YTD_newcar',lit(0))  
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.withColumn('MTD_newcar',lit(0))  
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.withColumn('YTD_pr',lit(0.0))  
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.withColumn('MTD_pr',lit(0.0))  
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.drop('Month_cw','Week_cw')



        sale_pr_weekly_YTD = sale_pr_weekly_YTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                        
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('Month',lit(str(max_month)))
        sale_pr_weekly_YTD = sale_pr_weekly_YTD.withColumn('Week',lit(str(max_week)))
        
                    
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                          
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.filter(sale_pr_weekly_MTD.Month == str(max_month))
        sale_pr_weekly_MTD = sale_pr_weekly_MTD.withColumn('Week',lit(str(max_week)))
                  

        sale_pr_weekly_WTD = sale_pr_weekly_WTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                          
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        sale_pr_weekly_WTD = sale_pr_weekly_WTD.filter(sale_pr_weekly_WTD.Week == str(max_week))
        
        sale_pr_weekly = sale_pr_weekly_YTD.unionAll(sale_pr_weekly_MTD).unionAll(sale_pr_weekly_WTD) 
        
        
        sale_pr_weekly.registerTempTable("sale_pr_weekly")
        
        sale_pr_weekly = spark.sql("""select Region,Brand,Year,Month,Week,Product_EN,Product_Line,
        YTD_contractsale,MTD_contractsale,WTD_contractsale,YTD_newcar,MTD_newcar,
        WTD_newcar,YTD_pr,MTD_pr,WTD_pr,YTD_Revenue,MTD_Revenue,WTD_Revenue,Start_Date,End_Date,
        case when DataType='Total_mtd' then 'MTD' when DataType='Total_ytd' 
        then 'YTD' when DataType='Total_wtd' then 'WTD' else DataType end as DataType from sale_pr_weekly""")
        
        sale_pr_weekly.registerTempTable("sale_pr_weekly")
        
        print("truncate table")
        spark.sql("truncate table revr_bmbs_srv_contract_blue.tgt_pr_weekly_info_revenue")
        print("insert data")
        spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_pr_weekly_info_revenue select * from sale_pr_weekly")


        return sale_pr_weekly 


    def _get_isp_pr_weekly(self,isp_blue_contract,isp_silver_contract):
        
        isp_total_sale = self._get_isp_total_sale(isp_blue_contract,isp_silver_contract)
        #they do not provide the data before 2018
        isp_total_sale = isp_total_sale.filter(isp_total_sale.Year >= '2018').withColumnRenamed('Product','Product_Line')

        #generate new column month_numb to prepare for joining table week，for example: '2018-03-01' -> '201803'
        cesar_week = spark.sql("""select case when brand == 'MB' then 'Mercedes-Benz' 
               else 'Smart' end as brand,
               region,wtd,mtd,ytd,week,cast(start_date as string) start_date,
               cast(end_date as string) end_date from 
               revr_bmbs_srv_contract_blue.src_cesar_week_info """)
       
        print("Rename column")
        schema = StructType([StructField("Brand", StringType(), True)
                            ,StructField("Region", StringType(), True)
                            ,StructField("WTD", IntegerType(), True)
                            ,StructField("MTD",IntegerType(),True)
                            ,StructField("YTD", IntegerType(), True)
                            ,StructField("Week", IntegerType(), True)
                            ,StructField("Start_Date", StringType(), True)
                            ,StructField("End_Date", StringType(), True)])
            
        cesar_week = spark.createDataFrame(cesar_week.rdd, schema)
        
        func_get_year = udf(get_year,StringType())
        cesar_week = cesar_week.withColumn('Year',func_get_year('End_Date'))
        
        func_get_month = udf(get_month,StringType())
        cesar_week = cesar_week.withColumn('Month',func_get_month('End_Date'))
       
       
        col_names = {'WTD':'WTD_newcar','MTD':'MTD_newcar','YTD':'YTD_newcar'} 
        for col_name in col_names:
            cesar_week = cesar_week.withColumnRenamed(col_name,col_names[col_name])     
            
            
        week_list = cesar_week.drop_duplicates(['Week']).select('Week').toPandas()
        week_list['Week'] = week_list['Week'].astype(str)            

        for num in range(len(week_list)):
            tmp_table = cesar_week.filter(cesar_week.Week == week_list.Week[num])
            start_date = tmp_table.drop_duplicates(['Start_Date']).select('Start_Date').toPandas().Start_Date[0]
            end_date = tmp_table.drop_duplicates(['End_Date']).select('End_Date').toPandas().End_Date[0]
            diff = int(end_date) - int(start_date)
            tmp_start_date = start_date
            if num == 0:
                for i in range(diff+1):
                    if i == 0:
                        j=0
                        final = tmp_table.withColumn('Mapping_Date',lit(int(start_date)+j))
                        
                    else:
                        j=1
                        tmp_start_date = int(tmp_start_date) + j 
                        tmp = tmp_table.withColumn('Mapping_Date',lit(int(tmp_start_date)))
                        final = final.unionAll(tmp)
            else:
                j=0
                for x in range(diff+1):
                    tmp_start_date = int(tmp_start_date) + j
                    tmp_table = tmp_table.withColumn('Mapping_Date',lit(int(tmp_start_date)))
                    j=1
                    final = final.unionAll(tmp_table)
                        
        cesar_week = final
        
        func_change_date = udf(change_date,DateType())
        cesar_week = cesar_week.withColumn('Mapping_Date', func_change_date('Mapping_Date'))
        cesar_week.cache()
        cesar_week = cesar_week.withColumnRenamed('Region','Region_cw')
        cesar_week = cesar_week.withColumnRenamed('Brand','Brand_cw')


        cond = [isp_total_sale.Region == cesar_week.Region_cw, isp_total_sale.Brand == cesar_week.Brand_cw, isp_total_sale.Signature_Date == cesar_week.Mapping_Date]
        isp_total_sale_weekly = isp_total_sale.join(cesar_week.select(['Brand_cw','Region_cw',\
        'WTD_newcar','MTD_newcar','YTD_newcar','Week','Start_Date','End_Date','Mapping_Date']),cond, how = 'left')

        isp_total_sale_weekly_ym = isp_total_sale_weekly.groupby(['Product_EN','Product_Line',\
                                                 'Year',\
                                                 'Month',\
                                                 'Week',\
                                                 'Brand',\
                                                 'Region']).agg({'ContractNumber':'count','Value':'sum'})
    
    
        rename_cols = {'count(contractnumber)':'SalesVolume','sum(Value)':'Revenue'}
        for i in rename_cols:
            isp_total_sale_weekly_ym = isp_total_sale_weekly_ym.withColumnRenamed(i,rename_cols[i])
                
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_ym.groupby(['Year',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','YTD_contractsale')\
            .withColumnRenamed('sum(Revenue)','YTD_Revenue')
          
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_ym.groupby(['Year','Month',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','MTD_contractsale')\
            .withColumnRenamed('sum(Revenue)','MTD_Revenue')
            
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_ym.groupby(['Year','Month','Week',\
        'Region','Brand','Product_EN','Product_Line'])\
        .agg({'SalesVolume':'sum','Revenue':'sum'}).withColumnRenamed('sum(SalesVolume)','WTD_contractsale').withColumnRenamed('sum(Revenue)','WTD_Revenue')
        
        
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('MTD_contractsale',lit(0))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('MTD_Revenue',lit(0))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('WTD_contractsale',lit(0))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('WTD_Revenue',lit(0))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('Month',lit(''))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('Week',lit(''))
        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.withColumn('DataType',lit('YTD'))
        

        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('YTD_contractsale',lit(0))
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('YTD_Revenue',lit(0))            
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('WTD_contractsale',lit(0))
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('WTD_Revenue',lit(0))
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('Week',lit(''))
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.withColumn('DataType',lit('MTD'))

        
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('YTD_contractsale',lit(0))
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('YTD_Revenue',lit(0))          
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('MTD_contractsale',lit(0))
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('MTD_Revenue',lit(0))

        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('Week',isp_total_sale_weekly_WTD.Week.cast('String'))
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.withColumn('DataType',lit('WTD'))
        

        isp_total_sale_weekly_YTD = isp_total_sale_weekly_YTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])        
        isp_total_sale_weekly_MTD = isp_total_sale_weekly_MTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        isp_total_sale_weekly_WTD = isp_total_sale_weekly_WTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])

        
        
        #isp_total  is not added up by the four region volume in cesar_week
        print("generate isp_total  table") 
        isp_completed_sale_YTD = isp_total_sale_weekly_YTD.groupby(['Year','Brand','Product_EN','Product_Line'])\
                                               .agg({'YTD_contractsale':'sum','YTD_Revenue':'sum'})

        isp_completed_sale_MTD = isp_total_sale_weekly_MTD.groupby(['Year','Month','Brand','Product_EN','Product_Line'])\
                                               .agg({'MTD_contractsale':'sum','MTD_Revenue':'sum'})
            
        isp_completed_sale_WTD = isp_total_sale_weekly_WTD.groupby(['Year','Month','Week','Brand','Product_EN','Product_Line'])\
                                               .agg({'WTD_contractsale':'sum','WTD_Revenue':'sum'})
         
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('Region',lit('Total'))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('Region',lit('Total'))
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('Region',lit('Total'))
        
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('Month',lit(''))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('Week',lit(''))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('MTD_contractsale',lit(0))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('MTD_Revenue',lit(0))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('WTD_contractsale',lit(0))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('WTD_Revenue',lit(0))
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumn('DataType',lit('Total_ytd'))


        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('Week',lit(''))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('YTD_contractsale',lit(0))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('YTD_Revenue',lit(0))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('WTD_contractsale',lit(0))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('WTD_Revenue',lit(0))
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumn('DataType',lit('Total_mtd'))


        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('YTD_contractsale',lit(0))
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('YTD_Revenue',lit(0))
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('MTD_contractsale',lit(0))
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('MTD_Revenue',lit(0))
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumn('DataType',lit('Total_wtd'))

#        
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumnRenamed('sum(YTD_contractsale)','YTD_contractsale')
        isp_completed_sale_YTD = isp_completed_sale_YTD.withColumnRenamed('sum(YTD_Revenue)','YTD_Revenue')
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumnRenamed('sum(MTD_contractsale)','MTD_contractsale')
        isp_completed_sale_MTD = isp_completed_sale_MTD.withColumnRenamed('sum(MTD_Revenue)','MTD_Revenue')
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumnRenamed('sum(WTD_contractsale)','WTD_contractsale')
        isp_completed_sale_WTD = isp_completed_sale_WTD.withColumnRenamed('sum(WTD_Revenue)','WTD_Revenue')
        
        isp_completed_sale_YTD = isp_completed_sale_YTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        isp_completed_sale_MTD = isp_completed_sale_MTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])
        isp_completed_sale_WTD = isp_completed_sale_WTD.select(['Year','Month','Week','Region','Brand','Product_EN','Product_Line',\
        'YTD_contractsale','MTD_contractsale','WTD_contractsale','YTD_Revenue','MTD_Revenue','WTD_Revenue','DataType'])

        
        isp_total_sale_YTD = isp_total_sale_weekly_YTD.unionAll(isp_completed_sale_YTD)
        isp_total_sale_MTD = isp_total_sale_weekly_MTD.unionAll(isp_completed_sale_MTD)
        isp_total_sale_WTD = isp_total_sale_weekly_WTD.unionAll(isp_completed_sale_WTD)


        print("generate sale_pr_weekly")
        cesar_week = spark.sql("""select case when brand == 'MB' then 'Mercedes-Benz' 
               else 'Smart' end as brand,
               region,wtd,mtd,ytd,week,cast(start_date as string) start_date,
               cast(end_date as string) end_date from 
               revr_bmbs_srv_contract_blue.src_cesar_week_info """)
       
        print("Rename column")
        schema = StructType([StructField("Brand", StringType(), True)
                            ,StructField("Region", StringType(), True)
                            ,StructField("WTD", IntegerType(), True)
                            ,StructField("MTD",IntegerType(),True)
                            ,StructField("YTD", IntegerType(), True)
                            ,StructField("Week", IntegerType(), True)
                            ,StructField("Start_Date", StringType(), True)
                            ,StructField("End_Date", StringType(), True)])
            
        cesar_week = spark.createDataFrame(cesar_week.rdd, schema)
        
        func_get_year = udf(get_year,StringType())
        cesar_week = cesar_week.withColumn('Year',func_get_year('End_Date'))
        
        func_get_month = udf(get_month,StringType())
        cesar_week = cesar_week.withColumn('Month',func_get_month('End_Date'))
       
       
        col_names = {'WTD':'WTD_newcar','MTD':'MTD_newcar','YTD':'YTD_newcar'} 
        for col_name in col_names:
            cesar_week = cesar_week.withColumnRenamed(col_name,col_names[col_name])     
                      
        cesar_week = cesar_week.withColumnRenamed('Year','Year_cw')
        cesar_week = cesar_week.withColumnRenamed('Month','Month_cw')
        cesar_week = cesar_week.withColumnRenamed('Week','Week_cw')
        cesar_week = cesar_week.withColumnRenamed('Region','Region_cw')
        cesar_week = cesar_week.withColumnRenamed('Brand','Brand_cw')

        max_month = cesar_week.groupby().agg({'Month_cw':'max'}).toPandas()['max(Month_cw)'][0]
        max_week = cesar_week.groupby().agg({'Week_cw':'max'}).toPandas()['max(Week_cw)'][0]
        
        cesar_week = cesar_week.filter(cesar_week.Week_cw == str(max_week))        
      
        cond = [isp_total_sale_YTD.Region == cesar_week.Region_cw, isp_total_sale_YTD.Brand == cesar_week.Brand_cw, isp_total_sale_YTD.Year == cesar_week.Year_cw]
        isp_sale_pr_weekly_YTD = isp_total_sale_YTD.join(cesar_week.select(['Region_cw','Brand_cw','Year_cw','YTD_newcar']),cond,'left')

        cond = [isp_total_sale_MTD.Region == cesar_week.Region_cw, isp_total_sale_MTD.Brand == cesar_week.Brand_cw,\
                isp_total_sale_MTD.Year == cesar_week.Year_cw,isp_total_sale_MTD.Month==cesar_week.Month_cw]
        isp_sale_pr_weekly_MTD = isp_total_sale_MTD.join(cesar_week.select(['Region_cw','Brand_cw','Year_cw','Month_cw','MTD_newcar']),cond,'left')    

        cond = [isp_total_sale_WTD.Region == cesar_week.Region_cw, isp_total_sale_WTD.Brand == cesar_week.Brand_cw, \
                isp_total_sale_WTD.Year == cesar_week.Year_cw,isp_total_sale_WTD.Month==cesar_week.Month_cw,isp_total_sale_WTD.Week == cesar_week.Week_cw]
        isp_sale_pr_weekly_WTD = isp_total_sale_WTD.join(cesar_week.select(['Region_cw',\
        'Brand_cw','Year_cw','Month_cw','Week_cw','WTD_newcar','Start_Date','End_Date']),cond,'left')

        
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('YTD_pr',round((isp_sale_pr_weekly_YTD['YTD_contractsale']/isp_sale_pr_weekly_YTD['YTD_newcar']),4))
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('MTD_pr',round((isp_sale_pr_weekly_MTD['MTD_contractsale']/isp_sale_pr_weekly_MTD['MTD_newcar']),4))
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.withColumn('WTD_pr',round((isp_sale_pr_weekly_WTD['WTD_contractsale']/isp_sale_pr_weekly_WTD['WTD_newcar']),4))

        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('MTD_newcar',lit(0))  
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('WTD_newcar',lit(0))  
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('MTD_pr',lit(0.0))  
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('WTD_pr',lit(0.0))  
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('Start_Date',lit(''))  
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('End_Date',lit(''))  

        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('YTD_newcar',lit(0))  
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('WTD_newcar',lit(0))  
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('YTD_pr',lit(0.0))  
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('WTD_pr',lit(0.0))  
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('Start_Date',lit(''))  
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('End_Date',lit(''))         
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.drop('Month_cw')


        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.withColumn('YTD_newcar',lit(0))  
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.withColumn('MTD_newcar',lit(0))  
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.withColumn('YTD_pr',lit(0.0))  
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.withColumn('MTD_pr',lit(0.0))  
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.drop('Month_cw','Week_cw')


        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                        
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('Month',lit(str(max_month)))
        isp_sale_pr_weekly_YTD = isp_sale_pr_weekly_YTD.withColumn('Week',lit(str(max_week)))
        
                    
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                          
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.filter(isp_sale_pr_weekly_MTD.Month == str(max_month))
        isp_sale_pr_weekly_MTD = isp_sale_pr_weekly_MTD.withColumn('Week',lit(str(max_week)))
                  

        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.select(['Region'
                                                        ,'Brand'
                                                        ,'Year'
                                                        ,'Month'
                                                        ,'Week'
                                                        ,'Product_EN'
                                                        ,'Product_Line'
                                                        ,'YTD_contractsale'
                                                        ,'MTD_contractsale'
                                                        ,'WTD_contractsale'
                                                        ,'YTD_newcar'
                                                        ,'MTD_newcar'
                                                        ,'WTD_newcar'
                                                        ,'YTD_pr'
                                                        ,'MTD_pr'
                                                        ,'WTD_pr'
                                                        ,'YTD_Revenue'
                                                        ,'MTD_Revenue'                                                          
                                                        ,'WTD_Revenue'
                                                        ,'Start_Date'
                                                        ,'End_Date'
                                                        ,'DataType'])
        
        isp_sale_pr_weekly_WTD = isp_sale_pr_weekly_WTD.filter(isp_sale_pr_weekly_WTD.Week == str(max_week))
        
        isp_sale_pr_weekly = isp_sale_pr_weekly_YTD.unionAll(isp_sale_pr_weekly_MTD).unionAll(isp_sale_pr_weekly_WTD) 
        
        
        isp_sale_pr_weekly.registerTempTable("isp_sale_pr_weekly")
        
        isp_sale_pr_weekly = spark.sql("""select Region,Brand,Year,Month,Week,Product_EN,Product_Line,
        YTD_contractsale,MTD_contractsale,WTD_contractsale,YTD_newcar,MTD_newcar,
        WTD_newcar,YTD_pr,MTD_pr,WTD_pr,YTD_Revenue,MTD_Revenue,WTD_Revenue,Start_Date,End_Date,
        case when DataType='Total_mtd' then 'MTD' when DataType='Total_ytd' 
        then 'YTD' when DataType='Total_wtd' then 'WTD' else DataType end as DataType from isp_sale_pr_weekly""")
        
        isp_sale_pr_weekly.registerTempTable("isp_sale_pr_weekly")
        
        print("truncate table")
        spark.sql("truncate table revr_bmbs_srv_contract_blue.tgt_isp_pr_weekly_info_bkp")
        print("insert data")
        spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_isp_pr_weekly_info_bkp select * from isp_sale_pr_weekly")


        return isp_sale_pr_weekly 




