
from pyspark.sql.functions import to_timestamp,col,date_format,when

class SQLTransformation:

    def __init__(self,sqlContext,df):
        self.sqlContext=sqlContext
        df.registerTempTable("covid_table")

    def transform_for_metric_total_cases(self):
        return self.sqlContext.sql("SELECT  \
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

    def transform_for_gcc_states(self,df):
        gcc = ['BH', 'KW', 'OM', 'QA', 'SA', 'AE']
        df = df.withColumn("dimensions", when(col("location").isin(gcc), "GCC").otherwise(col("dimensions")))
        ids = ['BH', 'KW', 'OM', 'QA', 'SA', 'AE']
        gcc_states_record = df.filter(df.location.isin(ids)).groupBy("month").pivot("location").sum(
            "total_cases")
        df = df.groupBy("month").pivot("dimensions").sum("total_cases")
        df = gcc_states_record.join(df, on=['month'], how='outer')
        return df