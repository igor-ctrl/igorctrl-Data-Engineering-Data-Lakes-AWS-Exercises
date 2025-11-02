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

# Script generated for node AccelerometerLandingNode
AccelerometerLandingNode_node1762047129348 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingNode_node1762047129348")

# Script generated for node CustomerTrustedNode
CustomerTrustedNode_node1762047179761 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1762047179761")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
      accel.user,
      accel.timestamp,
      accel.x,
      accel.y,
      accel.z
  FROM myDataSource accel
  INNER JOIN myDataSource1 customer
  ON accel.user = customer.email
'''
SQLQuery_node1762047159187 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AccelerometerLandingNode_node1762047129348, "myDataSource1":CustomerTrustedNode_node1762047179761}, transformation_ctx = "SQLQuery_node1762047159187")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1762047159187, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762047125265", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1762047365993 = glueContext.getSink(path="s3://stedi-lake-house-igor-tch/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1762047365993")
AmazonS3_node1762047365993.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1762047365993.setFormat("json")
AmazonS3_node1762047365993.writeFrame(SQLQuery_node1762047159187)
job.commit()