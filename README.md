# Project: STEDI Human Balance Analytics

## Project Instructions
Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

## Implementation

### Landing Zone

**Customer Landing**:
[customer_landing.sql](script/customer_landing.sql)
<figure>
  <img src="image/customer_landing.PNG" alt="Customer Landing" width=60% height=60%>
</figure>

**Accelerometer Landing**:
[accelerometer_landing.sql](script/accelerometer_landing.sql)
<figure>
  <img src="image/accelerometer_landing.PNG" alt="Accelerometer Landing" width=60% height=60%>
</figure>

### Trusted Zone

**Customer Trusted**:
[customer_landing_to_trusted.py](script/customer_landing_to_trusted.py)
[customer_trusted.sql](script/customer_trusted.sql)
<figure>
  <img src="image/customer_trusted.PNG" alt="Customer Trusted" width=60% height=60%>
</figure>

**Accelerometer Trusted**:
[accelerometer_landing_to_trusted.py](script/accelerometer_landing_to_trusted.py)
[accelerometer_trusted.sql](script/accelerometer_trusted.sql)
<figure>
  <img src="image/accelerometer_trusted.PNG" Accelerometer Trusted" width=60% height=60%>
</figure>

### Curated Zone

**Customer Curated**:
[customer_trusted_to_curated.py](script/customer_trusted_to_curated.py)
[customers_curated.sql](script/customers_curated.sql)
<figure>
  <img src="image/customers_curated.PNG" alt="Customer Curated" width=60% height=60%>
</figure>

**Step Trainer**:
[step_trainer_landing_to_trusted.py](script/step_trainer_landing_to_trusted.py)

**Machine Learning Curated**:
[machine_learning_curated.py](script/machine_learning_curated.py)