from pyspark.sql.functions import to_timestamp,col,date_format,when
from sqlTransformation import SQLTransformation


class Transformer(SQLTransformation):

    def __init__(self, df, sqlcontxt):
        self.df = df
        self.sqlContext = sqlcontxt

    def data_types_transformations(self):
        self.df = self.df.withColumn("date", to_timestamp(col('date'), 'yyyy-MM-dd'))
        self.df = self.df.withColumn("total_cases", self.df["total_cases"].cast("integer"))
        self.df = self.df.withColumn("month", date_format(col("date"), "yyyyMM"))

    def dimensions_transfomations(self):
        sqltranformation = SQLTransformation(self.sqlContext, self.df)
        data = sqltranformation.transform_for_metric_total_cases()
        data = sqltranformation.transform_for_gcc_states(data)
        return data

    def fill_na(self,df):
        return df.na.fill(0)
