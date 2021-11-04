# Databricks notebook source
# MAGIC %md # Test Harness for Concurrency Testing

# COMMAND ----------

# MAGIC %pip install databricks-sql-connector

# COMMAND ----------

import time
import os
import random
from databricks import sql

# COMMAND ----------

token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get()
os.environ["DATABRICKS_TOKEN"] = token

def build_connection(host, path, cache= 'true'):
  #Generate User Token
#   token = os.environ["DATABRICKS_TOKEN"]
  
  connection = sql.connect(
    server_hostname=host,
    http_path=path,
    access_token=token,
    configuration={'use_cached_result':cache})
  
  return connection

# COMMAND ----------

def generate_query():
  # WHERE Predicate columns
  cut = ['Ideal','Premium','Very Good','Good','Fair'] #Use LIST for categorical variables
  clarity = ['IF','SI1','VS2','I1','SI2','VVS2','VVS1','VS1'] 
  price = {'min': 1000, 'max': 9999} #Use DICT for min/max of continuous variables
  #Dictionary of all values to replace and corresponding list/dict of replacements
  vals_to_replace = {'CUT_VALUE':cut, 'CLARITY_VALUE':clarity, 'PRICE_VALUE':price}
  # Base Query to modify
  base_query = """
  SELECT 
    cut, 
    color, 
    clarity, 
    depth, 
    avg(cast(carat as DOUBLE)) as avg_carat, 
    max(cast(table as DOUBLE)) as max_table,
    min(cast(price as INT)) as min_price
  FROM default.diamonds
  WHERE cut == 'CUT_VALUE'
  AND clarity = 'CLARITY_VALUE'
  AND price > PRICE_VALUE
  GROUP BY 1,2,3,4
  ORDER BY min_price ASC
  """

  #String-substitute randomized values
  gen_query = base_query
  for key, value in vals_to_replace.items():
    if type(value) == list:
      gen_query = gen_query.replace(key, random.choice(value))
    elif type(value) == dict:
      gen_query = gen_query.replace(key, str(random.randint(price['min'], price['max'])))

  #Return resulting randomly generated query
  print(gen_query)
  
  return gen_query.replace('\n', ' ')

# COMMAND ----------

def run_query(connection, query):
  # Distribute to workers
  
  cursor = connection.cursor()

  tic = time.perf_counter()
  cursor.execute(query)
#   cursor.execute('SELECT * FROM <database-name>.<table-name> LIMIT 2')
  result = cursor.fetchall()
  toc = time.perf_counter()

  print(f"Query ran in {toc - tic:0.4f} seconds")
  dur = toc - tic
  
  num_results = len(result)

#   for row in result:
#     print(row)

  cursor.close()
  return dur, num_results

# COMMAND ----------

# host = '...'
# http_path = '...'

# con = build_connection(host, http_path)
# cursor = con.cursor()
# cursor.execute('SELECT * FROM default.diamonds LIMIT 12')
# result = cursor.fetchall()
# # for row in result:
# #     print(row)

# # query = generate_query()
# # run_query(con, query)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, LongType, StringType, IntegerType, TimestampType
schema = StructType([StructField('id', LongType(), True), 
                     StructField('query', StringType(), True),
                     StructField('client_ip', StringType(), True),
                     StructField('worker_cores', IntegerType(), True),
                     StructField('duration_sec', DoubleType(), True),
                     StructField('num_results', IntegerType(), True),
                     StructField('start_milli_utc', DoubleType(), True),
                     StructField('end_milli_utc', DoubleType(), True),
                     StructField('start_ts', TimestampType(), True),
                     StructField('end_ts', TimestampType(), True),
                    ])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def run_sql_trials(test_matrix):
  #Each worker node needs all libraries
  import time
  import os
  import random
  import pandas as pd
  from databricks import sql
  
  host = '...'
  http_path = '...'
  connection = build_connection(host, http_path)
  
  query = generate_query()
  start, start_ts = int(round(time.time() * 1000)), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  duration, num_results = run_query(connection, query)
  end, end_ts = int(round(time.time() * 1000)), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  print("DURATION::",duration)
  
  test_matrix['query'] = query
  test_matrix['client_ip'] = os.environ['SPARK_LOCAL_IP']
  test_matrix['worker_cores'] = len(os.sched_getaffinity(0))
  test_matrix['duration_sec'] = duration
  test_matrix['num_results'] = num_results
  test_matrix['start_milli_utc'] = start
  test_matrix['end_milli_utc'] = end
  test_matrix['start_ts'] = pd.to_datetime(start_ts)
  test_matrix['end_ts'] = pd.to_datetime(end_ts)
  
  return test_matrix

# COMMAND ----------

ids = 100
test_matrix = spark.range(ids)
tic = time.perf_counter()  

concurrency = test_matrix.groupBy('id').apply(run_sql_trials).collect()
# .withColumn('test',F.from_unixtime(F.col('start_milli_utc')/1000).alias('tc'))
dur = time.perf_counter() - tic
print("Ran {} concurrent queries over {} seconds".format(ids, dur))

# COMMAND ----------

display(spark.createDataFrame(concurrency))

# COMMAND ----------

display(concurrency)

# COMMAND ----------

display(concurrency)

# COMMAND ----------


