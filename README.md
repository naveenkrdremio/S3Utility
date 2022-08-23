# S3Utility

Build 
mvn clean package

Run
java -jar target/s3-utility-1.0.0.jar <ASYNC/SYNC> <BUCKET_NAME> <PATH> <REGION> <ACCESS_KEY> <SECRET_KEY> 

eg: java -jar target/s3-utility-1.0.0.jar ASYNC test.com parquet_readers_benchmarks/compressed_10M.parquet us-west-2 ****** ***********