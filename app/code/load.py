
class Load:

    def __init__(self,df):
        self.df = df

    def load_data(self):

        pandas_df = self.df.toPandas()
        pandas_df.reset_index().drop('index', axis=1, inplace=True)
        pandas_df.to_json("/home/app/transformed_data/output.json", orient='records')