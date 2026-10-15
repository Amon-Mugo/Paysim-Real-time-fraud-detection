
from pyspark.sql import DataFrame,Window
from pyspark.sql import functions as F

row_num = "row_n"
mx= "transaction_id"

def mi(df:DataFrame,m:list=mx)-> int:
    g= Window.partitionBy(*mx)
    indexed = df.withColumn("row",F.row_number().over(g))
    clean = indexed.filter(F.col(row_num)== 1).drop(row_num)
    return clean

