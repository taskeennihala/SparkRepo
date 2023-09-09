from pyspark.sql import SparkSession
from SparkRepo.src.Assignment1.utils import *
# Installing sparksession
spark = SparkSession.builder.appName("SparkByAssignment").getOrCreate()

##csv file reading
users_df = spark.read.csv(r"C:\Users\nihalataskeen\PycharmProjects\Spark_repo\SparkRepo\resourse\user.csv",header=True, inferSchema=True)
transactions_df = spark.read.csv(r"C:\Users\nihalataskeen\PycharmProjects\Spark_repo\SparkRepo\resourse\transaction.csv",header=True, inferSchema=True)

product_locations,products,user_product_spending = product_analysis(users_df, transactions_df)

# Results
product_locations.show()
products.show()
user_product_spending.show()

# To show
transactions_df.show()
users_df.show()

# To end

spark.stop()