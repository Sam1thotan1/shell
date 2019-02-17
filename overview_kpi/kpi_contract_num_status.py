# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
        
from datetime import datetime
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import *


tmp_contract_plan = """
select contract_number as contractnumber,
planned_contract_end from revr_bmbs_srv_contract_blue.src_contract_info 

  """
  
contract_planend_GEWP = spark.sql(tmp_contract_plan)
cols = StructType([StructField('contractnumber',StringType(),True),
                           StructField('planned_contract_end',StringType(),True)])
contract_planend_GEWP = spark.createDataFrame(contract_planend_GEWP.rdd,cols)

tmp_contract = """
select contractnumber,region,owner_group,product_en,status
from revr_bmbs_srv_contract_blue.tgt_contract_essence_info

  """
contract_status_GEWP = spark.sql(tmp_contract)
cols = StructType([StructField('contractnumber', StringType(),True),
                            StructField('region',StringType(),True),
                           StructField('owner_group',StringType(),True),
                           StructField('product_en',StringType(),True),
                           StructField('status', StringType(),True)])
contract_status_GEWP = spark.createDataFrame(contract_essence.rdd,cols)

contract_status_GEWP = contract_status_GEWP.join(contract_planend_GEWP, "contractnumber", "left").select("contractnumber", "planned_contract_end",\
                     "region", "owner_group","product_en","status")
contract_status_GEWP = contract_status_GEWP.withColumn("planned_contract_end", contract_status_GEWP["planned_contract_end"].cast(IntegerType()))

contract_status_GEWP = contract_status_GEWP.filter('product_en == "GEWP 1+1" or product_en == "GEWP"')
contract_status_GEWP = contract_status_GEWP.filter('planned_contract_end >= 20190115')

contract_status_GEWP = contract_status_GEWP.withColumn("status", lit('In-Service'))
contract_status_GEWP = contract_status_GEWP.drop('planned_contract_end')

print("Load data in table contract_status_GEWP")
contract_status_GEWP.registerTempTable("contract_status_GEWP_all")
spark.sql("truncate table revr_bmbs_srv_contract_blue.contract_status_GEWP")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.contract_status_GEWP select * from contract_status_GEWP_all")

sql_text_contract = """
select region,owner_group,product_en,status, count(contractnumber) as contract_num 
from revr_bmbs_srv_contract_blue.tgt_contract_essence_info
where status='In-Service' group by region,owner_group,product_en,status
union all
select region,owner_group,product_en,status, count(contractnumber) as contract_num 
from revr_bmbs_srv_contract_blue.tgt_isp_contract_essence_info
where status='In-Service' group by region,owner_group,product_en,status
union all
select region,owner_group,product_en,status, count(contractnumber) as contract_num 
from revr_bmbs_srv_contract_blue.contract_status_GEWP
where status='In-Service' group by region,owner_group,product_en,status

        """
tgt_kpi_contract_num = spark.sql(sql_text_contract)
cols = StructType([StructField('Region',StringType(),True),
                           StructField('DealerGroup',StringType(),True),
                           StructField('Product',StringType(),True),
                           StructField('Status', StringType(),True),
                           StructField('Contract_num', IntegerType(),True)])
tgt_kpi_contract_num = spark.createDataFrame(tgt_kpi_contract_num.rdd,cols)

tgt_kpi_contract_num = tgt_kpi_contract_num.withColumn("Year", lit('2019'))
tgt_kpi_contract_num = tgt_kpi_contract_num.withColumn("Month", lit('01'))

tgt_kpi_contract_num = tgt_kpi_contract_num.select(['Year',
                                    'Month',
                                    'Region',
                                    'DealerGroup',
                                    'Product',
                                    'Status',
                                    'Contract_num'])

                                    
print("Load data in table tgt_kpi_contract_num")
tgt_kpi_contract_num.registerTempTable("tgt_kpi_contract_num_all")
spark.sql("truncate table revr_bmbs_srv_contract_blue.tgt_kpi_contract_num")
spark.sql("insert overwrite table revr_bmbs_srv_contract_blue.tgt_kpi_contract_num select * from tgt_kpi_contract_num_all")







