# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 16:25:05 2019

@author: jiajuwu
"""


import pandas as pd

"""
sales_volume and revenue
"""
#contract_essence=pd.read_csv("C:/Users/jiajuwu/Desktop/11111data/contract_essence.csv",sep='|')

contract_essence=spark.sql("""select substring(signature_date,1,4) year, 
substring(signature_date,6,2) month, region,owner_group,product_en,contractnumber,
value from revr_bmbs_srv_contract_blue.tgt_contract_essence_info""")

contract_essence=contract_essence.drop_duplicates(['ContractNumber'])

contract_essence.columns

#contract_essence['Month']=contract_essence['SignatureDate'].apply(lambda x: x[5:7])
#
#contract_essence['Year']=contract_essence['SignatureDate'].apply(lambda x: x[:4])
#
#contract_essence.Month.unique()

#tmp_salesVolume=contract_essence.groupby(['Year','Month','Region','Owner_Group','Product_EN']).agg({'ContractNumber':'count','Value':'sum'})

tmp_salesVolume=contract_essence.groupby(['year','month','region','owner_group','product_en']).agg({'contractnumber':'count','value':'sum'})

tmp_salesVolume.columns=['Sales_volume','Revenue']

tmp_salesVolume=tmp_salesVolume.withColumnRenamed('count(contractnumber)','sales_volume').withColumnRenamed('sum(value)','revenue')

tmp_salesVolume.registerTempTable('sv')

spark.sql("insert into table revr_bmbs_srv_contract_blue.tgt_kpi_salesVolume_revenue select * from sv")



"""
cost
"""

claim=spark.sql("""select substring(repair_date,1,4) year, 
substring(repair_date,6,2) month, service_region,service_group,product_en,claim_amount 
from revr_bmbs_srv_contract_blue.tgt_claim_all_info""")

tmp_cost=claim.groupby(['year','month','service_region','service_group','product_en']).agg({'claim_amount':'sum'})

tmp_cost=tmp_cost.withColumnRenamed('sum(claim_amount)','cost')

tmp_cost.registerTempTable('cs')

spark.sql("insert into table revr_bmbs_srv_contract_blue.tgt_kpi_cost select * from cs")



"""
penetration
"""

#pr=pd.read_csv("C:/Users/jiajuwu/Desktop/11111data/pr_monthly.csv",sep='|')

pr=spark.sql("select * from revr_bmbs_srv_contract_blue.tgt_pr_monthly_info")

pr.columns

#pr_contractsales=pr.drop_duplicates(['GSSN',
#                         'Year',
#                         'Month',
#                         'Baumuster_6',
#                         'Brand',
#                         'Class',
#                         'Model',
#                         'Product_EN',
#                         'Product_line'])

pr_contractsales=pr.drop_duplicates(['gssn',
                         'year',
                         'month',
                         'baumuster_6',
                         'brand',
                         'class',
                         'model',
                         'product_en',
                         'product_line'])


pr_contractsales=pr_contractsales.groupby(['year','month','region','ownergroup','product_en']).agg({'contractsales':'sum'})    

#pr_contractsales=pr_contractsales.groupby(['Year','Month','Region','OwnerGroup','Product_EN'])['ContractSales'].sum().reset_index()    

pr_carsales=pr.drop_duplicates(['gssn','year','month','brand'])

#pr_carsales=pr.drop_duplicates(['GSSN','Year','Month','Brand'])

pr_carsales = pr_carsales.groupby(['year','month','region','ownergroup','product_en']).agg({'vehiclesales':'sum'})

#pr_carsales = pr_carsales.groupby(['Year','Month','Region','OwnerGroup','Product_EN'])['VehicleSales'].sum().reset_index()

pr_final = pr_contractsales.join(pr_carsales,on=['year','month','region','ownergroup','product_en'],how='left')

#pr_final = pr_contractsales.merge(pr_carsales,on=['Year','Month','Region','OwnerGroup','Product_EN'],how='left')

pr_final=pr_final.withColumnRenamed('sum(contractsales)','contractsales').withColumnRenamed('sum(vehiclesales)','vehiclesales')
pr_final=pr_final.na.fill({'vehiclesales':0.0,'contractsales':0.0})

#pr_final['VehicleSales']=pr_final.VehicleSales.fillna(0.0)
#
#pr_final['ContractSales']=pr_final.ContractSales.fillna(0.0)

pr_final=pr_final.withColumn('penetration',(pr_final.contractsales/pr_final.vehiclesales))

#pr_final['Penetration']=pr_final.ContractSales/pr_final.VehicleSales

pr_final.loc[pr_final.VehicleSales==0.0,'penetration']=0.0

pr_final=pr_final.na.fill({'penetration':0.0})
 
pr_final.registerTempTable('pr')

spark.sql("insert into table revr_bmbs_srv_contract_blue.tgt_kpi_penetration select * from pr")

        

#"""
#through put
#"""
##def get_total_fin_by_sigYear(year_sig,year_rep):
##    
##    """
##    param:
##        year_sig: signature year,such as '2015'
##        year_rep: repair　year, such as '2015'
##    
##    result:
##        total fin by signature year and repair year: such as total fin 
##        in year_sig '2015' and year_rep '2015' (pyspark dataframe)
##        
##    """
##    
##    sql_text="""select d.year,d.month,d.service_region,d.service_group,d.product_en,count(d.fin) through_put from (select substring(c.signature_date,1,4) year,substring(c.signature_date,6,2) month,c.service_region,c.service_group,c.product_en,c.fin from (select a.*,b.service_group 
##    from revr_bmbs_srv_contract_blue.src_dms_bkp a left join revr_bmbs_srv_contract_blue.tgt_claim_all_info b on a.contractnumber=b.contractnumber 
##    and a.repair_date=b.repair_date) c where product_en ='GMP light' and year(signature_date)='""" + year_sig+ "' and year(repair_date)='" +year_rep +"""' and 
##    year(date_in)='"""+year_rep+"') d group by d.year,d.month,d.service_region,d.service_group,d.product_en"
##    
##    df_fin = spark.sql(sql_text)
##    
##    df_fin = df_fin.withColumn('year_sig',lit(year_sig)).withColumn('year_rep',lit(year_rep))
##    
##    df_fin = df_fin.select(['year_sig','year_rep','revenue'])
##    
##    return df_fin
#
##year_sig='2018'
##year_rep='2018'
##
##sql_text="""select d.year,d.month,d.service_region,d.service_group,d.product_en,count(d.fin) through_put from (select substring(c.signature_date,1,4) year,substring(c.signature_date,6,2) month,c.service_region,c.service_group,c.product_en,c.fin from (select a.*,b.service_group 
##from revr_bmbs_srv_contract_blue.src_dms_bkp a left join revr_bmbs_srv_contract_blue.tgt_claim_all_info b on a.contractnumber=b.contractnumber 
##and a.repair_date=b.repair_date) c where product_en ='GMP light' and year(signature_date)='""" + year_sig+ "' and year(repair_date)='" +year_rep +"""' and 
##year(date_in)='"""+year_rep+"') d group by d.year,d.month,d.service_region,d.service_group,d.product_en"
#
#import time
#from pyspark.sql.functions import lit
#
#current_time_str=time.strftime('%Y%m%d %H:%m',time.localtime())
#year_rep=current_time_str[:4]
#    
#sql_text="""select d.year,d.service_region,d.service_group,d.product_en,count(d.fin) 
#through_put from (select substring(c.repair_date,1,4) year,substring(c.repair_date,6,2) month,
#c.service_region,c.service_group,c.product_en,c.fin from (select a.*,b.service_group 
#from revr_bmbs_srv_contract_blue.src_dms_bkp a left join 
#revr_bmbs_srv_contract_blue.tgt_claim_all_info b on a.contractnumber=b.contractnumber 
#and a.repair_date=b.repair_date) c where year(repair_date)='""" +year_rep +"""' and 
#year(date_in)='"""+year_rep+"') d group by d.year,d.service_region,d.service_group,d.product_en"
#         
#df_fin = spark.sql(sql_text)
#
#month_rep=current_time_str[4:6]
#
#df_fin=df_fin.withColumn('month',lit(month_rep))
#
#df_fin=df_fin.withColumn('last_modified_date',lit(current_time_str))
#
#df_fin=df_fin.select(['service_region','service_group','product_en','through_put','last_modified_date'])
#
#df_fin.registerTempTable('df')
#
#spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_kpi_through_put_bkp partition(year=%s, month=%s) select * from df"%(year_rep,month_rep))
#
#
#
#
#















