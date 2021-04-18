
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import to_timestamp,col,date_format,when

if __name__=='__main__':


	spark = SparkSession \
		    .builder \
		    .appName("Covid App") \
		    .config("spark.some.config.option", "some-value") \
		    .getOrCreate()

	sqlContext = SQLContext(spark)


	df=spark.read \
    		.format("csv") \
    		.option("sep", ",") \
    		.option("inferSchema", "true") \
    		.option("header", "true") \
    		.load("/tmp/data/owid-covid-data.csv")

	df=df.withColumn("date", to_timestamp(col('date'), 'yyyy-MM-dd'))
	df=df.withColumn("total_cases", df["total_cases"].cast("integer"))
	df = df.withColumn("month", date_format(col("date"), "yyyyMM"))
	df.registerTempTable("covid_table")

	output = sqlContext.sql("SELECT  \
                         UPPER(continent) as dimensions,\
                        CASE\
                            WHEN UPPER(location) ='BAHRAIN'  THEN 'BH'\
                            WHEN UPPER(location) = 'KUWAIT' THEN 'KW'\
                            WHEN UPPER(location) = 'OMAN' THEN 'OM'\
                            WHEN UPPER(location) = 'QATAR' THEN 'QA'\
                            WHEN UPPER(location) = 'SAUDI ARABIA' THEN 'SA'\
                            WHEN UPPER(location) = 'UNITED ARAB EMIRATES' THEN 'AE'\
                            ELSE UPPER(location) \
                            END AS location,month,SUM(total_cases) AS total_cases \
                        FROM covid_table \
                        where UPPER(continent) IN ('AFRICA', 'ASIA', 'EUROPE', 'NORTH AMERICA', 'OCEANIA',\
                            'SOUTH AMERICA', 'GCC' , 'BH', 'KW', 'OM', 'QA', 'SA', 'AE') \
                        GROUP BY month,continent,location")

	gcc=['BH','KW','OM','QA','SA','AE']
	output=output.withColumn("dimensions",when(col("location").isin(gcc), "GCC").otherwise(col("dimensions")))

	ids = ['BH','KW','OM','QA','SA','AE']
	gcc_states_record=output.filter(output.location.isin(ids)).groupBy("month").pivot("location").sum("total_cases")


	continents=output.groupBy("month").pivot("dimensions").sum("total_cases")                                                                      


	output = gcc_states_record.join(continents, on=['month'], how='outer')
	output=  output.na.fill(0)


	pandas_df = output.toPandas()
	pandas_df.reset_index().drop('index', axis=1, inplace=True)
	pandas_df.to_json("/home/app/transformed_data/output.json",orient='records')
