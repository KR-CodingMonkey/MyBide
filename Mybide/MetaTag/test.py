from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import Row


datas1 = [("foo", 1), ("bar", 2)]
datas2 = [ Row(name='Alice', age=5, height=80),
           Row(name='Alice', age=5, height=80),
           Row(name='Alice', age=10, height=80)]

# Spark Context를 이용하는 방법 
sc.parallelize(datas1).toDF().show()
sc.parallelize(datas2).toDF().show()

# spark = SparkSession.builder.master('local').getOrCreate()