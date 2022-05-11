from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
import json

def processdf(df):
    sparkDF=spark.createDataFrame(df)
    #sparkDF.printSchema()
    print("spark dataframe")
    #sparkDF.show()
    print("Print rating descending")
            #sparkDF.schema().dataType
    #types=[f.dataType for f in sparkDF.schema.fields]
    #print(types)
    sparkDF=sparkDF.orderBy(["Price"], ascending=False)
    sortedData=sparkDF.toJSON().map(lambda j:json.loads(j)).collect()
    return json.dumps(sortedData)