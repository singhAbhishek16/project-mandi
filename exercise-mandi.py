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


# head = ps_df.head() # ye number of columns de dega. here, 10
# number_of_rows = ps_df.count() # ye number of rows de dega


# TODO: date ko date banao. MM/dd/YYYY format hai - done
# TODO: state, district name, market name, commodity, variety, grade - inn sabke unique values nikalo - done
# TODO: dekho kaha kaha null hai; kis columns me - done
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

# print(ps_df.describe())
# saare price wale columns ka mean, max, min + date ka kab se kab ka data hai - de diya
# STATE, District Name, Market Name, Commodity, Variety, Grade, Min_Price, Max_Price, Modal_Price, Price Date

# check ki kis column me kitne nulls hai
# print(ps_df.isnull().sum().sort_values(ascending=False))

# har columns ke unique values dekhlo
# unique_commodities = ps_df["Commodity"].unique().tolist()
# print("unique commodities:")
# print(unique_commodities)
# print(len(unique_commodities))
#
# unique_varieties = ps_df["Variety"].unique().tolist()
# print("unique varieties:")
# print(unique_varieties)
# print(len(unique_varieties))

# unique_grades = ps_df["Grade"].unique().tolist()
# print("unique grades:")
# print(unique_grades)
# print(len(unique_grades))


min_0 = ps_df[(ps_df['Min_Price']==0.0) & (ps_df['Max_Price']!=0.0) & (ps_df['Modal_Price']!=0.0)]
max_0 = ps_df[(ps_df['Max_Price']==0.0) & (ps_df['Min_Price']!=0.0) & (ps_df['Modal_Price']!=0.0)]
modal_0 = ps_df[(ps_df['Modal_Price']==0.0) & (ps_df['Max_Price']!=0.0) & (ps_df['Min_Price']!=0.0)]
min_max_0 = ps_df[(ps_df['Min_Price']==0.0) & (ps_df['Max_Price']==0.0) & (ps_df['Modal_Price']!=0.0)]
min_modal_0 = ps_df[(ps_df['Min_Price']==0.0) & (ps_df['Modal_Price']==0.0) & (ps_df['Max_Price']!=0.0)]
max_modal_0 = ps_df[(ps_df['Max_Price']==0.0) & (ps_df['Modal_Price']==0.0) & (ps_df['Min_Price']!=0.0)]
min_max_modal_0 = ps_df[(ps_df['Max_Price']==0.0) & (ps_df['Modal_Price']==0.0) & (ps_df['Min_Price']==0.0)]

print(f'rows with only min price as 0:\n{min_0}')
print(f'rows with only max price as 0:\n{max_0}')
print(f'rows with only modal price as 0:\n{modal_0}')

print(f'\nwhole dataset has {ps_df.shape} rows;\n{min_0.shape} are with 0 min;\n{max_0.shape} are with 0 max;\n{modal_0.shape} are with 0 modal;\n{min_max_0.shape} are with min and max 0;\n{min_modal_0.shape} are with min and modal 0;\n{max_modal_0.shape} are with max and modal 0;\n{min_max_modal_0.shape} are with min, max and modal 0;')
# OP:whole dataset has (737392, 10) rows;
# (65, 10) are with 0 min;
# (564, 10) are with 0 max;
# (1, 10) are with 0 modal;
# (47, 10) are with min and max 0;
# (2, 10) are with min and modal 0;
# (0, 10) are with max and modal 0;
# (0, 10) are with min, max and modal 0;

# problem observed later: when we are counting min_price=0; its also getting counted in min_price=0&modal_price=0 and min_price=0&max_price=0
# approach steps:
# 1. if all three 0, delete
# 2. if two of them 0 & one strictly !0, handle them
# 3. if only one is 0, two other strictly !0, handle them

ps_df_cleanup = ps_df


ps_df_cleanup = ps_df_cleanup[
    ~(
        (ps_df_cleanup['Min_Price'] == 0.0) &
        (ps_df_cleanup['Max_Price'] == 0.0) &
        (ps_df_cleanup['Modal_Price'] == 0.0)
    )
]
print(f'\nnumber of rows with after dropping values where all three prices were 0: {ps_df_cleanup.shape}')


# ps_df_cleanup.loc[ps_df_cleanup['Min_Price']==0.0, 'Min_Price'] = 9.27
# ps_df_cleanup.loc[ps_df_cleanup['Max_Price'] == 0.0, 'Max_Price'] = ps_df_cleanup[['Min_Price', 'Modal_Price']].min(axis=1)
# new_min_0 = ps_df_cleanup[ps_df_cleanup['Min_Price']==9.27]
# print(f'rows with min price as 0:\n{new_min_0}')

spark.stop()


'''
what is done until now?
- read csv and make sense of columns
- converted price from string to float
- converted date from string to datetype
- find nulls per column
- mean/min/max of price columns
- unique values for commodities, varieties

what answers i am looking for?
- which mandi is offering the best price for their crop today, and how has that price trended over weeks?
- which mandis consistently offer the best price for a given commodity, and where do price anomalies (sudden spikes/drops) occur?

VVI TODO: last me 2 alag notes banana - ek approach and kaise decide kiya kya clean karna hai; aur second code ka

'''