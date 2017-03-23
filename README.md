# CloudFileSystemFuS3FS
The open source project FuS3FS is a course project of Cloud Computing Class in the University of Notre Dame. The contributors are Chao Luo and Kaijun Feng. FuS3FS is a cloud file system. All files are stored in Amazon S3, and distributed local cache is used to enhance I/O performance. Amazon SNS and SQS are employed to invalidate stale cache. 

Please follow the instructions below to use FuS3FS. Enjoy your time with FuS3FS!  


(1) Log in your AWS console, give permissions of S3, SNS, and SQS to your EC2 Instance (Use IAM role to give access to S3/SNS/SQS resources to EC2 Instances)

(2) Create a SNS topic and obtain your SNS topic ARN

(3) Mount an S3 bucket as a local folder using Amazon SNS and SQS

./main.py s3://YOUR/S3/BUCKET/NAME YOUR/LOCAL/FOLDER --topic SNS-TOPIC-ARN --new-queue


(4) Unmount a folder

fusermount -u YOUR/LOCAL/FOLDER



<p align="center">
  <img src="https://cloud.githubusercontent.com/assets/16144725/24264593/4f1285a8-0fd7-11e7-8a46-dd2eb93db166.png" width="200"/>
  <img src="https://cloud.githubusercontent.com/assets/16144725/24264604/53aba888-0fd7-11e7-9395-b25d6c12d6fe.png" width="200"/>
</p>
