import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://binh-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1683604534307 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://binh-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1683604534307",
)

# Script generated for node Renamed keys for Customer Privacy Filter
RenamedkeysforCustomerPrivacyFilter_node1683622920187 = ApplyMapping.apply(
    frame=CustomerCurated_node1683604534307,
    mappings=[
        ("serialNumber", "string", "`(customer) serialNumber`", "string"),
        ("birthDay", "string", "`(customer) birthDay`", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "`(customer) shareWithPublicAsOfDate`",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "`(customer) shareWithResearchAsOfDate`",
            "bigint",
        ),
        ("registrationDate", "bigint", "`(customer) registrationDate`", "bigint"),
        ("customerName", "string", "`(customer) customerName`", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "`(customer) shareWithFriendsAsOfDate`",
            "bigint",
        ),
        ("email", "string", "`(customer) email`", "string"),
        ("lastUpdateDate", "bigint", "`(customer) lastUpdateDate`", "bigint"),
        ("phone", "string", "`(customer) phone`", "string"),
    ],
    transformation_ctx="RenamedkeysforCustomerPrivacyFilter_node1683622920187",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforCustomerPrivacyFilter_node1683622920187,
    keys1=["serialNumber"],
    keys2=["`(customer) serialNumber`"],
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1683620377333 = DropFields.apply(
    frame=CustomerPrivacyFilter_node2,
    paths=[
        "`(customer) serialNumber`",
        "`(customer) birthDay`",
        "`(customer) shareWithPublicAsOfDate`",
        "`(customer) shareWithResearchAsOfDate`",
        "`(customer) registrationDate`",
        "`(customer) customerName`",
        "`(customer) shareWithFriendsAsOfDate`",
        "`(customer) email`",
        "`(customer) lastUpdateDate`",
        "`(customer) phone`",
    ],
    transformation_ctx="DropFields_node1683620377333",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683620377333,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://binh-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
