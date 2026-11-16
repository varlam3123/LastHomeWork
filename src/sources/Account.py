from pyspark.sql import functions as F


def credit_account(col):
    return col.cast('string').startswith('40802')

def debit_account(col):
    return col.cast('string').startswith('40702')




