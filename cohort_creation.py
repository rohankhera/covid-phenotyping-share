# Prerun the loading of required programs

from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lower #added lower here so can be referred below
import pandas as pd
import numpy as np
#The F.col creates a column for function
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .getOrCreate()
    
base_src = "/projects/cch/covid-phenotyping/omop-research/20210226/"

person = spark.read.parquet(base_src + "person")
measurement = spark.read.parquet(base_src + "measurement")
drug = spark.read.parquet(base_src + "drug_exposure")
condition = spark.read.parquet(base_src + "condition_occurrence")



PATH_BASE = '/projects/cch/covid-phenotyping/omop-research/20210301' #was updated date
PATH_CONDITION = PATH_BASE + '/condition_occurrence'

PATH_MEASUREMENTS = PATH_BASE + '/measurement'
condition = spark.read.parquet(PATH_CONDITION)

covid_cohort = condition.filter(
  (lower(col('condition_source_value')) == 'u07.1') & 
  (col('condition_status_concept_id') == 32902)
)

condition_status_concept_id_pd = condition_status_concept_id.select("*").toPandas() 


#Sort data frame by person 
covid_cohort = covid_cohort.sort(col("person_id").asc(), col("condition_start_datetime").asc())

#creating inpatient file with cum_count or groupBy counter - will take the first row
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

covid_cohort_order_label = covid_cohort.withColumn("diagnosis_order", \
              row_number().over(Window.partitionBy("person_id").orderBy("person_id", "condition_start_datetime")))


covid_cohort_dedup = covid_cohort_order_label.dropDuplicates(subset = ['person_id'])

#Second cohort based on all diagnosis

covid_cohort_all_dx = condition.filter(
  (lower(col('condition_source_value')) == 'u07.1')
)


covid_cohort_all_dx = covid_cohort_all_dx.sort(col("person_id").asc(), col("condition_start_datetime").asc())

covid_cohort_order_label_all_dx = covid_cohort_all_dx.withColumn("diagnosis_order", \
              row_number().over(Window.partitionBy("person_id").orderBy("person_id", "condition_start_datetime")))


covid_cohort_all_dx_dedup = covid_cohort_order_label_all_dx.dropDuplicates(subset = ['person_id'])


covid_cohort_dedup_pd = covid_cohort_dedup.select("*").toPandas()
covid_cohort_all_dx_dedup_pd = covid_cohort_all_dx_dedup.select("*").toPandas()

condition_march = condition.filter(col("condition_start_datetime") > '2020-03-01')

condition_march = condition_march.withColumn("B9721", F.when(F.col('condition_source_value') == "B97.21", 1).otherwise(0))
condition_march = condition_march.withColumn("B9729", F.when(F.col('condition_source_value') == "B97.29", 1).otherwise(0))
condition_march = condition_march.withColumn("B342", F.when(F.col('condition_source_value') == "B34.2", 1).otherwise(0))
condition_march = condition_march.withColumn("J1281", F.when(F.col('condition_source_value') == "J12.81", 1).otherwise(0))
condition_march = condition_march.withColumn("A4189", F.when(F.col('condition_source_value') == "A41.89", 1).otherwise(0))
condition_march = condition_march.withColumn("B9721", F.when(F.col('condition_source_value') == "B97.21", 1).otherwise(0))
condition_march = condition_march.withColumn("COVID_related_dx", (F.col('B9721') + F.col('B9729') + F.col('B342') +  F.col('J1281') + F.col('A4189')))
condition_march_sel = condition_march.filter(F.col("COVID_related_dx") > 0)

condition_march_lower = condition_march.withColumn('B9721_lower', F.when(F.col('condition_source_value') == "b97.21", 1).otherwise(0))
condition_march_lower = condition_march_lower.withColumn('B9729_lower', F.when(F.col('condition_source_value') == "b97.29", 1).otherwise(0))
condition_march_lower = condition_march_lower.withColumn('B342_lower', F.when(F.col('condition_source_value') == "b34.2", 1).otherwise(0))
condition_march_lower = condition_march_lower.withColumn('J1281_lower', F.when(F.col('condition_source_value') == "j12.81", 1).otherwise(0))
condition_march_lower = condition_march_lower.withColumn('A4189_lower', F.when(F.col('condition_source_value') == "a41.89", 1).otherwise(0))


condition_march.groupBy(col('B9721')).count().show()
condition_march.groupBy(col('B9729')).count().show()
condition_march.groupBy(col('B342')).count().show()
condition_march.groupBy(col('J1281')).count().show()
condition_march.groupBy(col('A4189')).count().show()
condition_march.groupBy(col('COVID_related_dx')).count().show()

condition_march_sel = condition_march.filter(F.col("COVID_related_dx") > 0)

condition_march_sel_pd = condition_march_sel.select("*").toPandas()

condition_march_sel_ordered = condition_march_sel.withColumn("diagnosis_order", \
              row_number().over(Window.partitionBy("person_id").orderBy("person_id", "condition_start_datetime")))

condition_march_sel_ordered_dedup = condition_march_sel_ordered.filter(col('diagnosis_order') == 1)

condition_march_sel_ordered_dedup_pd = condition_march_sel_ordered_dedup.select("*").toPandas()

condition_march_sel_ordered_dedup_pd_merge = condition_march_sel_ordered_dedup_pd[['person_id', 'condition_start_datetime','B9721', 'B9729', 'B342', 'J1281',
       'A4189', 'COVID_related_dx']]

condition_march_sel_ordered_dedup_pd_merge = condition_march_sel_ordered_dedup_pd_merge.rename(columns={'condition_start_datetime': 'covid_related_dx_start_datetime'})


covid_cohort_dedup_pd.columns
covid_cohort_dedup_pd_merge = covid_cohort_dedup_pd[['person_id', 'condition_start_datetime', 'diagnosis_order']]
covid_cohort_dedup_pd_merge = covid_cohort_dedup_pd_merge.rename(columns={'condition_start_datetime': 'primary_covid_dx_start_datetime', 'diagnosis_order': 'pri_covid_dx_yes'})

covid_cohort_all_dx_dedup_pd.columns
covid_cohort_all_dx_dedup_pd_merge = covid_cohort_all_dx_dedup_pd[['person_id', 'condition_start_datetime', 'diagnosis_order']]
covid_cohort_all_dx_dedup_pd_merge = covid_cohort_all_dx_dedup_pd_merge.rename(columns={'condition_start_datetime': 'any_covid_dx_start_datetime', 'diagnosis_order': 'any_covid_dx_yes'})

from functools import reduce
combined_covid_file = reduce(lambda x,y: pd.merge(x,y, on='person_id', how='outer'), [covid_cohort_dedup_pd_merge, covid_cohort_all_dx_dedup_pd_merge, condition_march_sel_ordered_dedup_pd_merge])
combined_covid_file.to_csv("~/data/combined_covid_file_2021_03_01.csv")

lrr = ['9074','26056','26030','26023','26022',
       '25978','25952','25940','25936','25861']
external = ['26074']
misc = ['14956'] #Requires more testing

covid_measurements = measurements.filter(
  col('measurement_source_concept_id').isin(lrr+external)
  )

'''
presumptive positive                                                                                                                                                                                                                                          |
positive 
positive/detected
positive sars-cov-2 
positive for 2019-novel coronavirus (2019-ncov) by pcr.  
detected sars-cov-2 (covid-19)  
positive for 2019-ncov  
presumptive positive for 2019-ncov   
2019-ncov rna detected 
detected
sars-cov-2 detected
presumptive pos
symptomatic
rna detected
the specimen is presumptively positive for sars-cov-2, the coronavirus associated with covid-19. this result was unable to be confirmed as positive by a second test method.
'''

pos_strings = ['presumptive positive',
              'positive',
              'positive/detected',
              'positive sars-cov-2',
              'positive for 2019-novel coronavirus (2019-ncov) by pcr.',
              'detected sars-cov-2 (covid-19)',
              'positive for 2019-ncov',
              'presumptive positive for 2019-ncov',
              '2019-ncov rna detected',
              'detected',
              'sars-cov-2 detected',
              'presumptive pos',
              'symptomatic',
               'rna detected',
              'the specimen is presumptively positive for sars-cov-2, the coronavirus associated with covid-19. this result was unable to be confirmed as positive by a second test method.']

covid_measurements_positive = measurements.filter(
(col('measurement_source_concept_id').isin(lrr+external))
  &
  (lower(col('value_source_value')).isin(pos_strings))
)


from pyspark.sql.functions import row_number 
from pyspark.sql.window import Window 

covid_measurements_positive_cohort_entry_datetime = covid_measurements_positive.withColumn("measurement_order",
              row_number().over(
              Window.partitionBy("person_id").orderBy(
              "person_id", "measurement_datetime")))

covid_measurements_positive_cohort_entry_datetime.show(2,False)

measurement_cohort = covid_measurements_positive_cohort_entry_datetime.filter(
col('measurement_order') == 1
).select('person_id', 'measurement_datetime')


measurements_table = measurement_cohort.select("*").toPandas() 
diag_table = pd.read_csv('./data/combined_covid_file_2021_03_01.csv', index_col=None)

measurements_table = measurements_table.drop('Unnamed: 0', axis=1)
diag_table = diag_table.drop('Unnamed: 0', axis=1)

lab_dx = np.ones((measurements_table.shape[0],1))

measurements_table['lab_dx'] = lab_dx

cohort_table = diag_table.merge(measurements_table, on='person_id', how='outer')
merged_measurement_diag_cohort_2021_03_01_pd = cohort_table

base_src = "/projects/cch/covid-phenotyping/omop-research/20210301/"
person = spark.read.parquet(base_src + "person")
person_pd = person.select("*").toPandas()

merged_person_diag_measure_pd_2021_03_01 = merged_measurement_diag_cohort_2021_03_01_pd.merge(person_pd, on = 'person_id', how = 'left')

#Export to csv
merged_person_diag_measure_pd_2021_03_01.to_csv("~/data/merged_person_diag_measure_pd_2021_03_01.csv")

