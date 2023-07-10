# BigData GoogleCloud Assignments
These are assignments of Big Data course

## GA 1:
* Provision a Virtual Machine (VM)
* Create a Python script to calculate the number of lines in a file stored in Google Cloud Storage (GCS)
* Include a text file with the output

## GA 2:
* Develop a Python program using Google Cloud Functions to count the lines of a file stored in GCS
* Include a text file with the output

## GA 3:
* Implement a Spark code to perform a hashing example on the publicly available file: gs://bucket_two_2/hash_file.txt
* Determine the count of user clicks within the time intervals: 0-6, 6-12, 12-18, and 18-24
* Include a text file with the output

## GA 4:
* Build a PySpark code to apply SCD Type II on a customer master data frame
* Provide a Python file containing the code
* Include a text file with the output

## GA 5:
* Utilize SparkSQL code to apply SCD Type II on a customer master data frame
* Provide a Python file containing the code
* Include a text file with the output

## GA 6:
* Real-time counting of lines in a file uploaded to a GCS bucket using Google Cloud Functions and Pub/Sub
* Create a Google Cloud Function triggered whenever a file is added to a bucket, which publishes the file name to a Pub/Sub topic
* Develop a Python file to subscribe to the topic and display the number of lines in the file in real-time

## GA 7:
* Stream data from a GCS bucket to Kafka in batches of 10 records, separated by a 10-second sleep time, until 100 records are written
* Utilize Spark Streaming to read from Kafka every 5 seconds and report the count of rows observed in the last 10 seconds

## GA 8:
* Modify the provided Spark MLlib code from https://docs.databricks.com/_extras/notebooks/source/decision-trees.html to incorporate the CrossValidator for automatic tuning
Report on the best performing model parameters
