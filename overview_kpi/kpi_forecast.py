# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 15:41:07 2019

@author: jiajuwu
"""

import time
from pyspark.sql.functions import date_format

current_time_str=time.strftime('%Y%m%d %H:%m',time.localtime())
year_rep=current_time_str[:4]
month_rep=current_time_str[4:6]


sql_text1="""select region,owner_group,product_en,fin,substring(next_service_date_up,1,10) next_service_date_up,
cast(substring(days_to_next_service_up,1,3) as int) days_to_next_service_up,cast(remainservice-1 as int) as remainservice
from revr_bmbs_srv_contract_blue.tgt_contract_essence_info where 
product_en in ('GMP','GMP light') and status != 'Normally End' 
and substring(days_to_next_service_up,3,1) != 'd' and next_service_date_up <='2019-12-31'and next_service_date_up >='2018-12-31' 
and cast(substring(days_to_next_service_up,1,3) as int) <= 365"""

#sql_text2="""select region,owner_group,product_en,fin,substring(next_service_date_up,1,10) next_service_date_up,
#cast(substring(days_to_next_service_up,1,2) as int) days_to_next_service_up,cast(remainservice-1 as int) as remainservice
#from revr_bmbs_srv_contract_blue.tgt_contract_essence_info where 
#product_en in ('GMP','GMP light') and status != 'Normally End' 
#and substring(days_to_next_service_up,3,1) == 'd' and next_service_date_up <='2019-12-31' and cast(substring(days_to_next_service_up,1,3) as int) <= 365"""             
#             

data=spark.sql(sql_text1)

#data1=spark.sql(sql_text2)
             
data_next_1 = data.groupby(['region','owner_group','product_en']).agg({'fin':'count'}).withColumnRenamed('count(fin)','next_through_put')

data=data.withColumn('next_service_date_int',(date_format('next_service_date_up','yyyyMMdd')))

from dateutil.relativedelta import relativedelta
from pyspark.sql.types import DateType,StringType,FloatType,IntegerType,StructType,StructField
from pyspark.sql.functions import udf, datediff, col, isnull, lit
from datetime import datetime

def days_add(service_date,days):
    
    service_date = datetime.strptime(str(service_date),'%Y-%m-%d')
    
    next_service_date = service_date+relativedelta(days=days)
    return next_service_date

func_days_add = udf(days_add,DateType())

data=data.withColumn('next_2_service_date',func_days_add(data.next_service_date_up,data.days_to_next_service_up))

data=data.withColumn('next_3_service_date',func_days_add(data.next_2_service_date,data.days_to_next_service_up))

data=data.withColumn('next_4_service_date',func_days_add(data.next_3_service_date,data.days_to_next_service_up))

data=data.withColumn('next_5_service_date',func_days_add(data.next_4_service_date,data.days_to_next_service_up))


data_pd=data.toPandas()

data_pd = data_pd.loc[(data_pd.remainservice>0)&(data_pd.next_2_service_date.astype(str)<'2019-12-31')]
data_pd_2 = data_pd.groupby(['region','owner_group','product_en']).agg({'fin':'count'}).reset_index().rename(columns={'fin':'next_2_through_put'})

i=0
data_pd = data_pd.loc[(data_pd.remainservice-1>0)&(data_pd.next_3_service_date.astype(str)<'2019-12-31')]
if data_pd.shape[0] !=0:
    data_pd_3 = data_pd.groupby(['region','owner_group','product_en']).agg({'fin':'count'}).reset_index().rename(columns={'fin':'next_3_through_put'})
    tmp_data_pd = data_pd_2.merge(data_pd_3,on=['region','owner_group','product_en'],how='left')
    tmp_data_pd = tmp_data_pd.fillna(0.0)
    tmp_data_pd['next_through_put_1']=tmp_data_pd['next_2_through_put']+tmp_data_pd['next_3_through_put']
    i+=1

    data_pd = data_pd.loc[(data_pd.remainservice-2>0)&(data_pd.next_4_service_date.astype(str)<'2019-12-31')]
    if data_pd.shape[0] !=0:
        data_pd_4 = data_pd.groupby(['region','owner_group','product_en']).agg({'fin':'count'}).reset_index().rename(columns={'fin':'next_4_through_put'})
        tmp_data_pd = tmp_data_pd.merge(data_pd_4,on=['region','owner_group','product_en'],how='left')
        tmp_data_pd = tmp_data_pd.fillna(0.0)
        tmp_data_pd['next_through_put_2']=tmp_data_pd['next_through_put_1']+tmp_data_pd['next_4_through_put']
        i+=1
        
        data_pd = data_pd.loc[(data_pd.remainservice-3>0)&(data_pd.next_5_service_date.astype(str)<'2019-12-31')]
        if data_pd.shape[0] !=0:
            data_pd_5 = data_pd.groupby(['region','owner_group','product_en']).agg({'fin':'count'}).reset_index().rename(columns={'fin':'next_5_through_put'})
            tmp_data_pd = tmp_data_pd.merge(data_pd_5,on=['region','owner_group','product_en'],how='left')
            tmp_data_pd = tmp_data_pd.fillna(0.0)
            tmp_data_pd['next_through_put_3']=tmp_data_pd['next_through_put_2']+tmp_data_pd['next_5_through_put']
            i+=1
        

tmp_data_pd=tmp_data_pd.loc[:,['region','owner_group','product_en','next_through_put_'+str(i)]].rename(columns={'next_through_put_'+str(i):'next_through_put_tmp'})

schema = StructType([StructField("region", StringType(), True)
,StructField("owner_group", StringType(), True)
,StructField("product_en", StringType(), True)
,StructField("next_through_put_tmp",FloatType(),True)])

tmp_sp = spark.createDataFrame(tmp_data_pd, schema)

tmp=data_next_1.join(tmp_sp,on=['region','owner_group','product_en'],how='left')
    
tmp=tmp.withColumn('through_put',(tmp.next_through_put+tmp.next_through_put_tmp))    

tmp=tmp.withColumn('last_modified_date',lit(current_time_str))

tmp=tmp.select(['region','owner_group','product_en','through_put','last_modified_date'])

tmp.registerTempTable('tmp')

spark.sql("""insert overwrite table revr_bmbs_srv_contract_blue.tgt_kpi_forecast 
partition(year=%s, month=%s) select * from tmp"""%(year_rep,month_rep))
                                                                               
                                                                               


                                                                               
                                                                               
















