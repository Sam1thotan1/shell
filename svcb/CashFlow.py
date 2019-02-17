# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 14:05:38 2018

@author: jiajuwu
"""
import pandas as pd

from pyspark.sql.functions import year,lit
from pyspark.sql.types import *



class Cash_Flow_Balance:
    
    def __init__(self,spark_context):
        self.sc = spark_context
    
    def get_cash_flow(self,blue_contract_esse,blue_cost_esse):
        
        blue_contract_esse = blue_contract_esse\
        .withColumn('Year',year(blue_contract_esse.Signature_Date).cast(StringType()))\
                   .withColumnRenamed('Service_Type','Product_Line')
        blue_cost_esse = blue_cost_esse\
        .withColumn('Year',year(blue_cost_esse.Repair_Date).cast(StringType()))\
                   .withColumnRenamed('Service_Type','Product_Line')
        
    
        blue_revenue = blue_contract_esse.groupby(['ContractNumber','Product_EN','Product_Line','Year']).agg({'Value':'sum'})
        
        blue_revenue = blue_revenue.withColumnRenamed('sum(Value)','Revenue')
        
        year_string_df = blue_contract_esse.select(['ContractNumber','Signature_Date','Year']).toPandas()
        
        year_list = year_string_df.Year.unique()

        blue_cost_esse=blue_cost_esse.select(['ContractNumber','Product_EN','Product_Line','Year','Claim_Amount']).toPandas()               
        for i in range(len(year_list)):
            if i ==0:
                print("processing %s year"%year_list[i])
                contract_year_df = year_string_df[year_string_df.Year== year_list[i]].ContractNumber
                blue_cost_year = blue_cost_esse[blue_cost_esse.ContractNumber\
                                                       .isin(contract_year_df)]
                
                
                blue_cost_year = blue_cost_year.groupby(['ContractNumber',\
                'Product_EN','Product_Line','Year' ]).agg({'Claim_Amount':'sum'}).reset_index()
                blue_cost_year = blue_cost_year.rename(columns={'Claim_Amount':'Cost'})
                
                print("create pivot table")
                blue_cost_year = blue_cost_year.pivot_table(blue_cost_year, index=['ContractNumber','Product_EN','Product_Line'], columns='Year')  
                blue_cost_year_pivoted = pd.DataFrame(blue_cost_year.to_records())
                blue_cost_year_pivoted.columns = [hdr.replace("('Cost', '", "Cost_").replace("')", "") for hdr in blue_cost_year_pivoted.columns]
                blue_cost_year_pivoted['Year'] = year_list[i]
                print(blue_cost_year_pivoted.columns)
            else:
                print("processing %s year"%year_list[i])
                contract_year_df = year_string_df[year_string_df.Year== year_list[i]].ContractNumber
                blue_cost_year_tmp = blue_cost_esse[blue_cost_esse.ContractNumber\
                                                       .isin(contract_year_df)]
                
                
                blue_cost_year_tmp = blue_cost_year_tmp.groupby(['ContractNumber',\
                'Product_EN','Product_Line','Year' ]).agg({'Claim_Amount':'sum'}).reset_index()
                blue_cost_year_tmp = blue_cost_year_tmp.rename(columns={'Claim_Amount':'Cost'})
                
                print("create pivot table")
                blue_cost_year_tmp = blue_cost_year_tmp.pivot_table(blue_cost_year_tmp, index=['ContractNumber','Product_EN','Product_Line'], columns='Year')  
                blue_cost_year_pivoted_tmp = pd.DataFrame(blue_cost_year_tmp.to_records())
                blue_cost_year_pivoted_tmp.columns = [hdr.replace("('Cost', '", "Cost_").replace("')", "") for hdr in blue_cost_year_pivoted_tmp.columns]
                print(blue_cost_year_pivoted_tmp.columns)
                print("check abnormal column")           
                check_col = pd.DataFrame({'df_col':['Cost_'+ x for x in year_list] })
                check_result = check_col[check_col.df_col.isin(blue_cost_year_pivoted_tmp.columns) == False]
                print("if existed replace 0")
                if len(check_result)>0:
                    for j in check_result.df_col:
                        blue_cost_year_pivoted_tmp[j]=0                                                
                                                                
                blue_cost_year_pivoted_tmp['Year'] = year_list[i]
                blue_cost_year_pivoted_tmp = blue_cost_year_pivoted_tmp[['ContractNumber','Product_EN','Product_Line']+[col for col in check_col.df_col]]
                blue_cost_year_pivoted_tmp['Year'] = year_list[i]
                print(blue_cost_year_pivoted.columns)
                print(blue_cost_year_pivoted_tmp.columns)
                print("union table")
                blue_cost_year_pivoted = blue_cost_year_pivoted.append(blue_cost_year_pivoted_tmp)               
                
        blue_cost_year = self.sc.createDataFrame(blue_cost_year_pivoted)

        blue_balance = blue_revenue.join(blue_cost_year,['ContractNumber','Product_EN','Product_Line','Year'],'left')
        blue_balance = blue_balance.na.fill(0)


        for i in range(len(year_list)):
            if i==0:
                blue_balance = blue_balance.withColumn(\
                'RemainBalance_'+year_list[i],(blue_balance.Revenue - \
                                          blue_balance[check_col.df_col[i]]))
            
            else:
                blue_balance = blue_balance.withColumn(\
                'RemainBalance_'+year_list[i],(blue_balance['RemainBalance_'+\
                                          year_list[i-1]]-blue_balance[check_col.df_col[i]]))
                    
        print("Load data in table tmp_blue_balance_info")
        blue_balance.registerTempTable("blue_balance")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_blue_balance_info")
        self.sc.sql("insert into table revr_bmbs_srv_contract_blue.tmp_blue_balance_info select * from blue_balance")
    
    
        return blue_balance


    def get_isp_cash_flow(self,isp_blue_contract_esse,isp_blue_cost_esse):
        
        #product line of blue is actual product line,but product line of isp is from service type 
        isp_blue_contract_esse = isp_blue_contract_esse\
        .withColumn('Year',year(isp_blue_contract_esse.Signature_Date).cast(StringType()))
#                   .withColumnRenamed('Service_Type','Product_Line')
        isp_blue_cost_esse = isp_blue_cost_esse\
        .withColumn('Year',year(isp_blue_cost_esse.Repair_Date).cast(StringType()))
#                   .withColumnRenamed('Service_Type','Product_Line')

        isp_blue_revenue = isp_blue_contract_esse.groupby(['ContractNumber','Product_EN','Product_line','Year']).agg({'Value':'sum'})
        
        isp_blue_revenue = isp_blue_revenue.withColumnRenamed('sum(Value)','Revenue')
        
       
        #persist before can help improve efficiency
        year_string_df = isp_blue_contract_esse.drop_duplicates(['ContractNumber','Signature_Date','Year']).select(['ContractNumber','Signature_Date','Year']).toPandas()       
        
        year_list = year_string_df.Year.unique()

        isp_blue_cost_esse=isp_blue_cost_esse.select(['ContractNumber','Product_EN','Product_line','Year','Claim_Amount']).toPandas()               
        for i in range(len(year_list)):
            if i ==0:
                print("processing %s year"%year_list[i])
                contract_year_df = year_string_df[year_string_df.Year== year_list[i]].ContractNumber
                isp_blue_cost_year = isp_blue_cost_esse[isp_blue_cost_esse.ContractNumber\
                                                       .isin(contract_year_df)]
                
                
                isp_blue_cost_year = isp_blue_cost_year.groupby(['ContractNumber',\
                'Product_EN','Product_line','Year' ]).agg({'Claim_Amount':'sum'}).reset_index()
                isp_blue_cost_year = isp_blue_cost_year.rename(columns={'Claim_Amount':'Cost'})
                
                print("create pivot table")
                isp_blue_cost_year = isp_blue_cost_year.pivot_table(isp_blue_cost_year, index=['ContractNumber','Product_EN','Product_line'], columns='Year')  
                isp_blue_cost_year_pivoted = pd.DataFrame(isp_blue_cost_year.to_records())
                isp_blue_cost_year_pivoted.columns = [hdr.replace("('Cost', '", "Cost_").replace("')", "") for hdr in isp_blue_cost_year_pivoted.columns]
                isp_blue_cost_year_pivoted['Year'] = year_list[i]
                print(isp_blue_cost_year_pivoted.columns)
            else:
                print("processing %s year"%year_list[i])
                contract_year_df = year_string_df[year_string_df.Year== year_list[i]].ContractNumber
                isp_blue_cost_year_tmp = isp_blue_cost_esse[isp_blue_cost_esse.ContractNumber\
                                                       .isin(contract_year_df)]

                #this part can happen that contract in 2019 has no cost in 2019,set this part cost as 0, 
                #in the later code we filter this part with left join         
                if isp_blue_cost_year_tmp.empty:
                    isp_blue_cost_year_tmp=isp_blue_cost_esse.loc[:,['ContractNumber','Product_EN','Product_line','Claim_Amount','Year']]
                    isp_blue_cost_year_tmp=isp_blue_cost_year_tmp.iloc[0:1,:]
                    tmp_columns=['ContractNumber','Product_EN','Product_line']
                    for tc in  tmp_columns:
                        isp_blue_cost_year_tmp['Year']= year_list[i]
                        isp_blue_cost_year_tmp[tc]='0'
                        isp_blue_cost_year_tmp['Claim_Amount']=0
                else:
                    isp_blue_cost_year_tmp=isp_blue_cost_year_tmp
                isp_blue_cost_year_tmp = isp_blue_cost_year_tmp.groupby(['ContractNumber',\
                'Product_EN','Product_line','Year' ]).agg({'Claim_Amount':'sum'}).reset_index()
                isp_blue_cost_year_tmp = isp_blue_cost_year_tmp.rename(columns={'Claim_Amount':'Cost'})
                
                print("create pivot table")
                isp_blue_cost_year_tmp = isp_blue_cost_year_tmp.pivot_table(isp_blue_cost_year_tmp, index=['ContractNumber','Product_EN','Product_line'], columns='Year')  
                isp_blue_cost_year_pivoted_tmp = pd.DataFrame(isp_blue_cost_year_tmp.to_records())
                isp_blue_cost_year_pivoted_tmp.columns = [hdr.replace("('Cost', '", "Cost_").replace("')", "") for hdr in isp_blue_cost_year_pivoted_tmp.columns]
                print(isp_blue_cost_year_pivoted_tmp.columns)
                print("check abnormal column")           
                check_col = pd.DataFrame({'df_col':['Cost_'+ x for x in year_list] })
                check_result = check_col[check_col.df_col.isin(isp_blue_cost_year_pivoted_tmp.columns) == False]
                print("if existed replace 0")
                if len(check_result)>0:
                    for j in check_result.df_col:
                        isp_blue_cost_year_pivoted_tmp[j]=0                                                
                                                                
                isp_blue_cost_year_pivoted_tmp['Year'] = year_list[i]
                isp_blue_cost_year_pivoted_tmp = isp_blue_cost_year_pivoted_tmp[['ContractNumber','Product_EN','Product_line']+[col for col in check_col.df_col]]
                isp_blue_cost_year_pivoted_tmp['Year'] = year_list[i]
                print(isp_blue_cost_year_pivoted.columns)
                print(isp_blue_cost_year_pivoted_tmp.columns)
                print("union table")
                isp_blue_cost_year_pivoted = isp_blue_cost_year_pivoted.append(isp_blue_cost_year_pivoted_tmp)
                
        isp_blue_cost_year = spark.createDataFrame(isp_blue_cost_year_pivoted)
        isp_blue_balance = isp_blue_revenue.join(isp_blue_cost_year,['ContractNumber','Product_EN','Product_line','Year'],'left')
        isp_blue_balance = isp_blue_balance.na.fill(0)

        for i in range(len(year_list)):
            if i==0:
                isp_blue_balance = isp_blue_balance.withColumn(\
                'RemainBalance_'+year_list[i],(isp_blue_balance.Revenue - \
                                          isp_blue_balance[check_col.df_col[i]]))
            
            else:
                isp_blue_balance = isp_blue_balance.withColumn(\
                'RemainBalance_'+year_list[i],(isp_blue_balance['RemainBalance_'+\
                                          year_list[i-1]]-isp_blue_balance[check_col.df_col[i]]))
                    
        print("Load data in table tmp_blue_balance_info")
        isp_blue_balance.registerTempTable("isp_blue_balance")
        self.sc.sql("truncate table revr_bmbs_srv_contract_blue.tmp_isp_blue_balance_info")
        self.sc.sql("insert into table revr_bmbs_srv_contract_blue._isp select * from isp_blue_balance")
 
        return isp_blue_balance

