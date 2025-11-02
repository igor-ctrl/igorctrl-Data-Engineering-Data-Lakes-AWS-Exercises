import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AccelerometerTrustedNode
AccelerometerTrustedNode_node1762050558435 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1762050558435")

# Script generated for node StepTrainerTrustedNode
StepTrainerTrustedNode_node1762050559676 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedNode_node1762050559676")

# Script generated for node SQL Query
SqlQuery875 = '''
  SELECT
      step.sensorReadingTime,
      step.serialNumber,
      step.distanceFromObject,
      accel.user,
      accel.x,
      accel.y,
      accel.z
  FROM myDataSource step
  INNER JOIN myDataSource1 accel
  ON step.sensorReadingTime = accel.timestamp
'''
SQLQuery_node1762050563926 = sparkSqlQuery(glueContext, query = SqlQuery875, mapping = {"myDataSource":StepTrainerTrustedNode_node1762050559676, "myDataSource1":AccelerometerTrustedNode_node1762050558435}, transformation_ctx = "SQLQuery_node1762050563926")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1762050563926, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762050555488", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1762050566091 = glueContext.getSink(path="s3://stedi-lake-house-igor-tch/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1762050566091")
AmazonS3_node1762050566091.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1762050566091.setFormat("json")
AmazonS3_node1762050566091.writeFrame(SQLQuery_node1762050563926)
job.commit()