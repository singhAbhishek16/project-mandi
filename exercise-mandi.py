'''
1. main kis question ka ans dhundra hu
2. csv me ek jhalak lo-
2.1 kitne rows hai; kitne colums hao; kitne type ke columns hai; un columns ka kya mtlb hai

'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import pyspark.pandas as ps
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

spark = SparkSession.builder \
    .appName("mandi-analysis") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# data_df = spark.read.csv("Agriculture_price_dataset.csv", header=True) # data is of type spark dataframe
# kyunki header csv me pehle se hai, we have to put header=True
ps_df = ps.read_csv("Agriculture_price_dataset.csv")


# head = data_df.head() # ye number of columns de dega. here, 10
# number_of_rows = data_df.count() # ye number of rows de dega
#
# print(f'head:\n {head}')
# print(f'number_of_rows:\n {number_of_rows}')

# TODO: date ko date banao. MM/dd/YYYY format hai - done
# TODO: state, district name, market name, commodity, variety, grade - inn sabke unique values nikalo
# TODO: dekho kaha kaha null hai; kis columns me
# TODO: saare prices string hai, unko number karo; date ko dateType karo - done

# data_with_formatted_date = data.withColumn("Price Date", func.to_date("Price Date", "MM/dd/yyyy"))
# to take a sneak peak in csv
# data_sneak_peak = data.take(5)
# for each_item in data_sneak_peak:
#     print(each_item)

ps_df["Min_Price"]=ps_df["Min_Price"].astype(float)
ps_df["Max_Price"]=ps_df["Max_Price"].astype(float)
ps_df["Modal_Price"]=ps_df["Modal_Price"].astype(float)
ps_df["Price Date"] = ps.to_datetime(ps_df["Price Date"])

print(ps_df.describe())
# saare price wale columns ka mean, max, min + date ka kab se kab ka data hai - de diya

spark.stop()