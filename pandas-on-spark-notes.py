'''
A. imports-
1. pyspark.sql se SparkSession
2. pyspark.pandas
3. os and set PYARROW_IGNORE_TIMEZONE
'''

from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps  # Import pandas-on-Spark
import pandas as pd

'''
B. initialize spark session; common to spark and pandas-on-spark
'''

spark = SparkSession.builder \
    .appName("Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

'''
C. spark-submit me logging level daldo; warna INFO logs bhar jate hai
'''
spark.sparkContext.setLogLevel("ERROR")


'''
D. ps.DataFrame - for pandas-on-spark dataframe
   pd.DataFrame - for pandas dataframe
'''

# pandas-on-spark dataframe
ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})

# pandas dataframe
pandas_df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45]
})

'''
E. to calculate mean of a column or describe
'''
ps_df["age"].mean()
ps_df.describe()


'''
F. add a new column based on some logic
'''
ps_df["salary_after_increment"] = ps_df["salary"] * 1.1 # salary column waisi he rahegi; salary_after_increment naya ban jayega

# Using `transform()` for element-wise operations
ps_df["age_in_10_years"] = ps_df["age"].transform(lambda x: x + 10)

# Using `apply()` on columns
def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"
# Apply the function to the 'salary' column
ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)


# Using `apply()` on rows
def format_row(row):
    return f"{row['name']} ({row['age']} years old)"
# Apply the function across rows
ps_df["name_with_age"] = ps_df.apply(format_row, axis=1)

'''
note for understanding-
- agar column pe operate karna hai
-- dataframe["column-name"].apply(function-name) (yaha apply ko bas ek column pe lagaya hai; mtlb uss column ka ek ek row function ko input jayega (essentially each element)
- agar row pe operate karna hai
-- dataframe.apply(function-name) (notice ki poora dataframe paas hua hai apply() ko; mtlb yaha ek ek karke poora row input hoga apply ke andar wale function ko)
'''

'''
G. filtering on a columns logic
'''
filtered_ps_df = ps_df[ps_df["age"] > 30]
filtered_spark_df = spark_df.filter(spark_df.age > 30)


'''
H. updating some values in a column based on external logic
         data['Brand']                        -> ye brand wale column de dega
         data['Brand']=='Tata'                -> ye true/false ki series de dega
data.loc[data['Brand']=='Tata']               -> ye 'tata' wale rows nikal kar de dega
data.loc[data['Brand']=='Tata', 'Mileage']=50 -> ye 'tata' wale rows ke 'Milege' values ko update kar dega
'''

'''
I. convertion. spark wale ke liye spark_df.show(); pandas-on-spark ke liye sedha print()
'''
spark_df = spark.createDataFrame(pandas_df)
ps_df = ps.DataFrame(pandas_df)

spark_df = ps_df.to_spark()

spark_df.show()

ps_df_from_spark = ps.DataFrame(spark_df)
converted_pandas_df = spark_df.toPandas()
print(ps_df_from_spark)