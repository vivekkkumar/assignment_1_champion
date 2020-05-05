def check_empty(x):
    return when(col(x) != "", col(x)).otherwise(None)


def csv_to_json(input_filename, output_filename):

	from pyspark.sql import SparkSession

	from pyspark.sql.functions import isnan, when, count, col

	from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

	spark = SparkSession\
	        .builder\
	        .appName("Write json")\
	        .master("local[*]")\
	        .getOrCreate()

	test_schema = StructType([
    	StructField("Person Id", IntegerType(), True),
    	StructField("Floor Access", DateType(), True),
    	StructField("Floor Level", IntegerType(), True),
    	StructField("Building", StringType(), True)    
	])

	csv_df = spark.read.option("header", "true").schema(test_schema).csv(input_filename)

	# cleaning the data and input proper null value to clean the dataset 
	# will be useful in later point of time by chaning the type to Null, like counting the Null.

	nan_cleaned_df = csv_df.select([when(isnan(c), c).otherwise(None).alias(c) for c in csv_df.columns]).show()

	expression = [check_empty(x).alias(x) for x in csv_df.columns]

	empty_to_null_df = csv_df.select(*expression)

	final_df = empty_to_null_df.na.drop(subset=[col(x) for x in csv_df.columns])

	final_df.coalesce(1).write.format('json').save(output_filename)


if __name__ == '__main__':

	import sys

	try:
		input_filename, output_filename = sys.argv[1:], sys.argv[2:]
	except:
		print ("usage: filename.py input_filename, output_filename")
		sys.exit(0)

	csv_to_json(input_filename, output_filename)
