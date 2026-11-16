from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pathlib import Path
from sources.Account import credit_account, debit_account
from utils.functions import get_table, get_money, save_table_postgresql
import os
import sys


os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder.appName("corporate_payments") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")\
    .getOrCreate()


path = Path(__file__).resolve().parent.parent

account = spark.read.parquet(f'{path}/data/SchemaBank/Account')
operation = spark.read.parquet(f'{path}/data/SchemaBank/Operation')
client = spark.read.parquet(f'{path}/data/SchemaBank/Client')
rate = spark.read.parquet(f'{path}/data/SchemaBank/Rate')
calculation_params_tech = spark.read.parquet(f'{path}/data/SchemaBank/calculation_params_tech')

cat1 = calculation_params_tech.filter(F.col('params') == 1).select('value').rdd.flatMap(lambda x: x).collect()
cat2 = calculation_params_tech.filter(F.col('params') == 2).select('value').rdd.flatMap(lambda x: x).collect()
windowSpec = Window.partitionBy(operation.columns).orderBy(F.col('RateDate').desc())



corporate_payments = account.join(operation, on=(account.AccountID == operation.AccountDB) | (account.AccountID == operation.AccountCR)) \
    .join(rate, on=(operation.Currency == rate.Currency) & (operation.DateOp >= rate.RateDate)).drop(rate.Currency) \
    .withColumn('ranks', F.dense_rank().over(windowSpec)).filter(F.col('ranks') == 1) \
    .join(client.filter(F.col('Type') == 'Ф').select('ClientId').distinct(), on='ClientId', how='left') \
    .withColumn('PaymentAmt', F.when(account.AccountID == operation.AccountDB, get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('EnrollementAmt', F.when(account.AccountID == operation.AccountCR, get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('TaxAmt', F.when((account.AccountID == operation.AccountDB) & (debit_account(account.AccountNum)),
                                 get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('ClearAmt', F.when((account.AccountID == operation.AccountDB) & (credit_account(account.AccountNum)),
                                   get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('CutoffDt', F.when(client.ClientId.isNotNull(), get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('CarsAmt', F.when(~F.col('comment').isin(cat1), get_money('Amount', 'Rate')).otherwise(0)) \
    .withColumn('FoodAmt', F.when(F.col('comment').isin(cat2), get_money('Amount', 'Rate')).otherwise(0)) \
    .groupBy(account.AccountID, account.ClientId, F.col('DateOp').alias('CutoffDt')).agg(
    F.sum('PaymentAmt').alias('PaymentAmt'),
    F.sum('EnrollementAmt').alias('EnrollementAmt'),
    F.sum('TaxAmt').alias('TaxAmt'),
    F.sum('ClearAmt').alias('ClearAmt'),
    F.sum('CarsAmt').alias('CarsAmt'),
    F.sum('FoodAmt').alias('FoodAmt'))


# save_table_postgresql(corporate_payments, 'corporate_payments')


corporate_payments.show(5)