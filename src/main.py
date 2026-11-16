# from posixpath import sep
#
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import DoubleType, StringType, DateType, StructField, StructType, IntegerType
# import os
# import sys
#
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ['PYSPARK_PYTHON'] = sys.executable
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName("SparkSession") \
#     .getOrCreate()




# rate_schema = StructType([
#     StructField('Currency', StringType()),
#     StructField('Rate', DoubleType()),
#     StructField('RateDate', DateType())
# ])
#
# operation_schema = StructType([
#     StructField('AccountDB', StringType()),
#     StructField('AccountCR', StringType()),
#     StructField('DateOp', DateType()),
#     StructField('Amount', StringType()),
#     StructField('Currency', StringType()),
#     StructField('Comment', StringType())
# ])
# #
# # Client = spark.read.csv(r'C:\newrep\Client.csv', header=True, inferSchema=True, sep=';')
# Account = spark.read.csv(r'C:\newrep\Account.csv', header=True, inferSchema=True, sep=';')
# Operation = spark.read.csv(r'C:\newrep\Operation.csv', header=True, sep=';')
# Rate = spark.read.csv(r'C:\newrep\Rate.csv', header=True, sep=';')


# Operation.withColumn("Amount", F.regexp_replace("Amount", ",", ".").cast("double")).show(5)

# Client.repartition(1, "RegisterDate")\
#     .write \
#     .partitionBy('RegisterDate') \
#     .mode("overwrite") \
#     .parquet(r'C:\newrep\SchemaBank\Client')
#
# Account.repartition(1, "DateOpen")\
#     .write \
#     .mode('overwrite') \
#     .partitionBy('DateOpen') \
#     .parquet(r'C:\newrep\SchemaBank\Account')
#
# Operation.repartition(1, "DateOp") \
#     .write \
#     .mode('overwrite')\
#     .partitionBy('DateOp') \
#     .parquet(r'C:\newrep\SchemaBank\Operation')
#
# Rate.repartition(1, "RateDate") \
#     .write \
#     .mode('overwrite') \
#     .partitionBy('RateDate') \
#     .parquet(r'C:\newrep\SchemaBank\Rate')

# string1 = '%а/м%, %а\м%, %автомобиль %, %автомобили %, %транспорт%, %трансп%средс%, %легков%, %тягач%, %вин%, %vin%,%viн:%, %fоrd%, %форд%,%кiа%, %кия%, %киа%%мiтsuвisнi%, %мицубиси%, %нissан%, %ниссан%, %sсанiа%, %вмw%, %бмв%, %аudi%, %ауди%, %jеер%, %джип%, %vоlvо%, %вольво%, %тоyота%, %тойота%, %тоиота%, %нyuнdаi%, %хендай%, %rенаulт%, %рено%, %реugеот%, %пежо%, %lаdа%, %лада%, %dатsuн%, %додж%, %меrсеdеs%, %мерседес%, %vоlкswаgен%, %фольксваген%, %sкоdа%, %шкода%, %самосвал%, %rover%, %ровер%'
#
# string2 = '%сою%, %соя%, %зерно%, %кукуруз%, %масло%, %молок%, %молоч%, %мясн%, %мясо%, %овощ%, %подсолн%, %пшениц%, %рис%, %с/х%прод%, %с/х%товар%, %с\х%прод%, %с\х%товар%, %сахар%, %сельск%прод%, %сельск%товар%, %сельхоз%прод%, %сельхоз%товар%, %семен%, %семечк%, %сено%, %соев%, %фрукт%, %яиц%, %ячмен%, %картоф%, %томат%, %говя%, %свин%, %курин%, %куриц%, %рыб%, %алко%, %чаи%, %кофе%, %чипс%, %напит%, %бакале%, %конфет%, %колбас%, %морож%, %с/м%, %с\м%, %консерв%, %пищев%, %питан%, %сыр%, %макарон%, %лосос%, %треск%, %саир%, % филе%, %хек%, %хлеб%, %какао%, %кондитер%, %пиво%, %ликер%'
#
#
#
#
# res = ([('cars', s.replace('%', '').strip()) for s in string1.split(',')] +
#        [('products', s.replace('%', '').strip()) for s in string2.split(',')])
#
#
#
# calculation_params_tech = spark.createDataFrame(res, ['params', 'value'])
#
# calculation_params_tech.repartition(1, 'params') \
#     .write \
#     .mode('overwrite') \
#     .partitionBy('params') \
#     .parquet(r'C:\newrep\SchemaBank\calculation_params_tech')


