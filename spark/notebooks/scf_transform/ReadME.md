## Spark Transformation Scripts and Docker Setup

Within the `spark` folder, I have placed the Spark scripts responsible for performing the raw data transformations. These scripts read data from the SeeClickFix folder in MinIO, select the necessary columns, drop unused ones, and create a `Date` column to capture the issue's exact date. Finally, the transformed data is saved back into MinIO as a CSV file containing the updated issues.

I have also created separate folders for Spark Worker and Master nodes, including the necessary JAR files to connect Docker to MinIO. These JAR files allow seamless data sharing between Spark and MinIO using the S3 protocol, fully compatible with MinIO.

Once the setup is complete, I will build the Docker image using the `Dockerfile` I have created. 

`docker build . -t airflow/issues_transformation`

This image will then be utilized in my Airflow pipeline via the `DockerOperator` to execute the Spark transformations and save the data.
