## Data Transformation Practice

As part of the larger pipeline, this session focuses on practicing data manipulation. Initially, I extracted and loaded raw data from SeeClickFix into my data lake (MIniO), following standard extraction and load mechanisms. 

In this session, I created a script to filter and select the exact data needed from the raw dataset. The script performs several key transformations:
1. Selecting the relevant columns based on the required properties.
2. Renaming the columns to ensure clarity and alignment with my needs.
3. Converting data types to ensure consistency across the dataset.
4. Saving the transformed data into a specified directory.

This transformed script will be used in a Spark job (see spark/notebook). I will containerize the script using Docker, building a custom Spark image. Later, I plan to use Airflow DockerOperator in my pipeline to call this Spark image, transform the data, and save it directly to MinIO.
