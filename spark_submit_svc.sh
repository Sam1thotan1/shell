
export SPARK_MAJOR_VERSION=2
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS
SPARK_HOME=/usr/hdp/current/spark2-client

${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 8g \
--num-executors 4 \
--executor-memory 10g \
--executor-cores 4 \
--files ${SPARK_HOME}/conf/hive-site.xml \
--py-files /data/a133_s_dlint02/service_contract_blue/svc.egg /data/a133_s_dlint02/service_contract_blue/service_contract_blue_main.py
























