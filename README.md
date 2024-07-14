import pandas as pd
from pyspark.sql import SparkSession
# File location and type
file_location = "/FileStore/tables/NCRB_Table_1C_2__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
spark_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
display()
#spark_df = spark_df.withColumnRenamed

spark_df = spark_df.withColumnRenamed('Sl. No.', 'Sl_No') \
    .withColumnRenamed('Murder (Sec.302 IPC)', 'Murder') \
    .withColumnRenamed('Culpable Homicide not amounting to Murder (Sec.304 IPC)', 'Culpable_Homicide') \
    .withColumnRenamed('Infanticide (Sec.315 IPC)', 'Infanticide') \
    .withColumnRenamed('Foeticide (Sec.316 IPC)', 'Foeticide') \
    .withColumnRenamed('Dowry Deaths (Sec.304B IPC)', 'Dowry_Deaths') \
    .withColumnRenamed('Attempt to Commit Murder (Sec.307 IPC)', 'Attempt_Murder') \
    .withColumnRenamed('Attempt to Commit Culpable Homicide (Sec.308 IPC)', 'Attempt_Culpable_Homicide') \
    .withColumnRenamed('Grievous Hurt (Sec 325, 326, 326A & 326B IPC)', 'Grievous_Hurt') \
    .withColumnRenamed('Kidnapping and Abduction (Sec 363-369 IPC)', 'Kidnapping_Abduction') \
    .withColumnRenamed('Rape (Sec.376 IPC)', 'Rape') \
    .withColumnRenamed('Attempt to Commit Rape (Sec.376 r/w 511 IPC)', 'Attempt_Rape') \
    .withColumnRenamed('Rioting (Sec 147-151 & 153A IPC)', 'Rioting') \
    .withColumnRenamed('Robbery (Sec 392 to 394 IPC)', 'Robbery') \
    .withColumnRenamed('Dacoity (Sec 395 to 398 IPC)', 'Dacoity') \
    .withColumnRenamed('Arson (Sec 435 to 438 IPC)', 'Arson') \
    .withColumnRenamed('Total Violent Crimes (Cols.3 to 17)', 'Total_Violent_Crimes')
spark_df.show()
# Create a Temporary View
spark_df.createOrReplaceTempView("crime_data")

# Data Analysis: Find Patterns and Trends
# Example: Get the total number of violent crimes for each state/UT
total_violent_crimes = spark.sql("""
    SELECT `State/UT` AS State_UT, SUM(Total_Violent_Crimes) AS Total_Violent_Crimes
    FROM crime_data
    GROUP BY State_UT
    ORDER BY Total_Violent_Crimes DESC
""")
total_violent_crimes.show()
spark.stop()
import matplotlib.pyplot as plt

# Convert the result to a Pandas DataFrame for visualization
total_violent_crimes_df = total_violent_crimes.toPandas()

# Plot
plt.figure(figsize=(14, 8))
plt.barh(total_violent_crimes_df['State_UT'], total_violent_crimes_df['Total_Violent_Crimes'], color='skyblue')
plt.xlabel('Total Violent Crimes')
plt.ylabel('State/UT')
plt.title('Total Violent Crimes by State/UT')
plt.gca().invert_yaxis()
plt.show()

# Stop the Spark session



