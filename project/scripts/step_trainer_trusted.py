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

# Script generated for node StepTrainerLandingNode
StepTrainerLandingNode_node1762050120839 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingNode_node1762050120839")

# Script generated for node CustomersCuratedNode
CustomersCuratedNode_node1762050122958 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="CustomersCuratedNode_node1762050122958")

# Script generated for node SQL Query
SqlQuery799 = '''
  SELECT
      step.sensorReadingTime,
      step.serialNumber,
      step.distanceFromObject
  FROM myDataSource step
  INNER JOIN myDataSource1 customer
  ON step.serialNumber = customer.serialNumber
'''
SQLQuery_node1762050164969 = sparkSqlQuery(glueContext, query = SqlQuery799, mapping = {"myDataSource1":CustomersCuratedNode_node1762050122958, "myDataSource":StepTrainerLandingNode_node1762050120839}, transformation_ctx = "SQLQuery_node1762050164969")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1762050164969, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762050085247", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1762050211999 = glueContext.getSink(path="s3://stedi-lake-house-igor-tch/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1762050211999")
AmazonS3_node1762050211999.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1762050211999.setFormat("json")
AmazonS3_node1762050211999.writeFrame(SQLQuery_node1762050164969)
job.commit()