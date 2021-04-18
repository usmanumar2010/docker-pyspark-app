
class Extract:

        def __init__(self, spark_context):
            self.spark=spark_context

        def extract_covid_data(self):
            df = self.spark.read \
                        .format("csv") \
                        .option("sep", ",") \
                        .option("inferSchema", "true") \
                        .option("header", "true") \
                        .load("/tmp/data/owid-covid-data.csv")
            return df