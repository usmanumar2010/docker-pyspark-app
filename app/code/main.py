import sys
sys.path.append("/home/app/code/")

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from extract import Extract
from transform import Transformer
from load import Load
if __name__=='__main__':


	spark = SparkSession \
				.builder \
				.appName("Covid App") \
				.config("spark.some.config.option", "some-value") \
				.getOrCreate()

	sqlContext = SQLContext(spark)

	df = Extract(spark)
	df = df.extract_covid_data()

	transformer = Transformer(df, sqlContext)
	transformer.data_types_transformations()
	transformed_df = transformer.dimensions_transfomations()
	transformed_df = transformer.fill_na(transformed_df)

	loader = Load(transformed_df)
	loader.load_data()





