from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils.functions import get_table, get_money, save_table_postgresql

spark = SparkSession.builder.appName("corporate_account") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()
path = Path(__file__).resolve().parent.parent

account = spark.read.parquet(f'{path}/data/SchemaBank/Account')
operation = spark.read.parquet(f'{path}/data/SchemaBank/Operation')
client = spark.read.parquet(f'{path}/data/SchemaBank/Client')
rate = spark.read.parquet(f'{path}/data/SchemaBank/Rate')
debit_table = operation.join(account, on=operation.AccountDB == account.AccountID) \
    .join(client, on='ClientID') \
    .select('AccountID', 'AccountNum', 'DateOpen', 'ClientId', 'ClientName', 'Amount', 'Currency', F.col('DateOp').alias('CutoffDt'))

credit_table = operation.join(account, on=operation.AccountCR == account.AccountID) \
    .join(client, on='ClientID') \
    .select('AccountID', 'AccountNum', 'DateOpen', 'ClientId', 'ClientName', 'Amount', 'Currency', 'DateOp')

final_columns = [c for c in debit_table.columns if c != 'Currency']
windowSpec = Window.partitionBy(final_columns).orderBy(F.col('RateDate').desc())
corporate_account = debit_table.union(credit_table).distinct() \
    .join(rate.hint('broadcast'), on=(debit_table.Currency == rate.Currency) & (debit_table.CutoffDt >= rate.RateDate)) \
    .withColumn('ranks', F.dense_rank().over(windowSpec)).filter(F.col('ranks') == 1) \
    .withColumn('Amount', F.sum(get_money(F.col('Amount'), F.col('Rate'))).over(windowSpec)) \
    .select(final_columns).withColumnRenamed('Amount', 'TotalAmt')

save_table_postgresql(corporate_account, 'corporate_account')

