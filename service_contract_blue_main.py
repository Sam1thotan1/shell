# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 11:25:17 2018

@author: jiajuwu
"""
import time
import pandas as pd
from datetime import datetime
from pyspark.sql.session import SparkSession
#import svcb.ReadHiveTable as rht
#import svcb.DataPrpare as dp
#import svcb.CostControl as cc
#import svcb.Forecast as fc
from pyspark.sql.types import *
import os
os.chdir("/data/a133_s_dlint02/service_contract_blue/develop/")
os.environ["NLS_LANG"] = ".AL32UTF8"

#export SPARK_DRIVER_MEMORY=8g
#export SPARK_EXCUTOR_MEMORY=10g    

#set spark
spark = SparkSession.builder \
    .appName("Service_Contract_Blue") \
    .config("hive.metastore.sasl.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

#define function printRunTime    
def printRunTime(ts):
    print("in %.2f s"%(time.perf_counter()-ts))
    ts=time.perf_counter()
    return ts

#initial start running time
ts=time.perf_counter()

year_string_list = ['2015','2016','2017','2018','2019']

Dmt = DealerMaster(spark)
dealer_info = Dmt.create_dealerid_info()
dealer_workshop = Dmt.create_workshop_info()
dealer_gssn = Dmt.create_gssn_info()        
dealer_sap = Dmt.create_dealer_sap_info()
dealer_change = Dmt.create_dealer_change_info()
gssn_worshop = Dmt.create_gssn_worshop_info()            


Pot = ProductInfo(spark)
product_silver_info = Dmt.create_silver_product_info()
product_isp_info = Dmt.create_isp_product_info()
product_blue_info = Dmt.create_blue_product_info()      


Pit = PriceInfo(spark)
isp_claim_price = Dmt.create_isp_price_claim_info()
isp_gap_price = Dmt.create_isp_gap_price_info()


Cnt = Contract_Info(spark,dealer_info,dealer_change)
Cst = Cost_Info(spark)
Cs = Cesar_Info(spark)

Pb = Prepare_Blue_Info(spark)
Ps = Prepare_Silver_Info(spark)
Pi = Prepare_ISP_Info(spark)

contract_info = Cnt._get_contract_info()

firstregday = contract_info.select(contract_info.ContractNumber,\
                                   contract_info.First_Registration_Date)

cost_info = Cst._get_cost_info(firstregday)

"""
Generate table contract_overviw and blue_cost_overview
"""
blue_contract,blue_cost = Pb._get_blue_overview(contract_info,\
                                                product_blue_info,\
                                                cost_info,\
                                                dealer_gssn,\
                                                dealer_workshop)

blue_cost_detect = Pb._get_blue_detect(blue_cost)
        
        
silver_contract,silver_cost = Ps._get_silver_overview(contract_info,\
                                                     product_silver_info,\
                                                     cost_info,\
                                                     dealer_gssn,\
                                                     dealer_workshop)

isp_blue_contract,isp_silver_contract, isp_blue_cost ,isp_silver_cost = Pi._get_isp_overview(contract_info,\
                                                                                             cost_info,\
                                                                                             product_isp_info,\
                                                                                             dealer_gssn,\
                                                                                             dealer_workshop)

isp_blue_cost_detect = Pi._get_isp_blue_detect(isp_blue_cost)


"""
Generate table blue_contract_essence and blue_cost_essence
"""
blue_contract_essence, blue_cost_essence = Pb._get_blue_esse(blue_contract,blue_cost)

"""
Generate table silver_contract_essence and silver_cost_essence
"""
silver_contract_essence, silver_cost_essence = Ps._get_silver_esse(silver_contract,silver_cost) 

"""
Generate table isp contract_essence and sip_cost_essence
"""        
isp_blue_contract_essence,isp_blue_cost_essence,isp_silver_contract_essence,isp_silver_cost_essence = Pi._get_isp_esse(isp_blue_contract,isp_silver_contract,isp_blue_cost,isp_silver_cost)

"""
Generate table dealsales_monthly, dealersales
"""
dealersales_monthly = Cs._get_cesar_info(year_string_list, dealer_sap)

"""
Generate table penetration_rate_monthly (pr_monthly)
and penetration_rate_yearly(pr_yearly)
from cesar data
"""
Pp = Prepare_PenetrationRate(year_string_list,dealersales_monthly,dealer_gssn,spark)

pr_monthly = Pp._get_pr_monthly(blue_contract,silver_contract)
pr_weekly = Pp._get_pr_weekly(blue_contract,silver_contract)        

isp_pr_monthly = Pp._get_isp_pr_monthly(isp_blue_contract,isp_silver_contract)
isp_pr_weekly = Pp._get_isp_pr_weekly(isp_blue_contract,isp_silver_contract)   


"""
cost control for each service
"""
claim_all = Cost_Control(spark)\
                                ._get_claim_control(blue_contract_essence,\
                                                    blue_cost_essence,
                                                    silver_contract_essence,\
                                                    silver_cost_essence,dealer_info)

print("Insert data into table tgt_blue_claim_all")
claim_all.registerTempTable("tmp_claim")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_claim_all_info_test select * from tmp_claim") 

isp_claim_all = Cost_Control(spark)\
                                ._get_isp_claim_control(isp_blue_contract_esse,\
                                                    isp_blue_cost_esse,
                                                    isp_silver_contract_esse,\
                                                    isp_silver_cost_esse,
                                                    gssn_worshop,
                                                    dealer_info)

print("Insert data into table tgt_blue_claim_all")
isp_claim_all.registerTempTable("tmp_isp_claim")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_isp_claim_all_info_test select * from tmp_isp_claim")
                                                
"""
cost control for total service 
"""
blue_contract_upperlimit = Cost_Control(spark)\
                                         ._get_contract_control(blue_contract_essence,\
                                                                blue_cost_essence)
                                         
print("Insert data into table tmp_blue_contract_upperlimit_info")
blue_contract_upperlimit.registerTempTable("tmp_upperlimit")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_blue_contract_upperlimit_info select * from tmp_upperlimit")                                                     


isp_contract_upperlimit = Cost_Control(spark)\
                                         ._get_isp_contract_control(isp_blue_contract_esse,\
                                                                isp_blue_cost_esse,\
                                                                isp_gap_price)
                                         
print("Insert data into table tmp_blue_contract_upperlimit_info")
isp_blue_contract_upperlimit.registerTempTable("tmp_isp_blue_upperlimit")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_isp_contract_upperlimit_info select * from tmp_isp_blue_upperlimit")                                                     
                       
"""
forcast next service date and create final table blue contract
"""
blue_contract_final = Prediction_Modelling(spark).forcast_nsd(blue_cost_detect,blue_cost_essence,blue_contract_upperlimit) 
blue_balance = Cash_Flow_Balance(spark).get_cash_flow(blue_contract_essence,blue_cost_essence)

print("Insert data into table tmp_blue_contract_final_info")
blue_contract_final.registerTempTable("tmp_blue_final")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_blue_contract_final_info select * from tmp_blue_final")    

isp_blue_contract_final = Prediction_Modelling(spark).forcast_nsd(isp_blue_cost_detect,isp_blue_cost_essence,isp_blue_contract_upperlimit) 
isp_blue_balance = Cash_Flow_Balance(spark).get_cash_flow(isp_blue_contract_essence,isp_blue_cost_essence)

print("Insert data into table tmp_blue_contract_final_info")
isp_blue_contract_final.registerTempTable("tmp_isp_blue_final")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tmp_blue_contract_final_info select * from tmp_isp_blue_final")    

"""
generate final contract essence through merging blue_contract_final,blue_balance and silver_contract_essence
"""
contract_essence = Final_Contract().get_contract_esse(blue_contract_final,blue_balance,silver_contract_essence)

print("Insert data into table tgt_contract_esse")
contract_essence.registerTempTable("tmp_contract")
spark.sql("insert into revr_bmbs_srv_contract_blue.tgt_contract_essence_info_test select * from tmp_contract")   

isp_contract_essence = Final_Contract().get_isp_contract_esse(isp_blue_contract_final,isp_blue_balance,isp_silver_contract_esse,contract)

print("Insert data into table tgt_contract_esse")
isp_contract_essence.registerTempTable("tmp_isp_contract")
spark.sql("insert into revr_bmbs_srv_contract_blue.tgt_isp_contract_essence_info_test select * from tmp_isp_contract")   












