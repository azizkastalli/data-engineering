## Introduction
The main goal of this project is to move the processes and data of a music streaming app (sparkify) onto the cloud.<br>
To do so, I created an EMR(Elastic MapReduce) cluster on aws. The cluster has hadoop and spark already installed.
Next, I extracted and loaded songs and log data from and to an s3 bucket and used pyspark to transform it into specific dimensions and fact tables so it can be used later for analytics.<br>
The data that resides in S3 contains JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Content and Structure
### 1. ERD

![image](https://drive.google.com/uc?export=view&id=1M5xNIgHgMI10vFy1Yl5WaM49qOWFC61u)

The transformed tables follows a star schema design such as <b>songplay</b> is the fact table. 
The dimension tables are as follows:
* <b>artists:</b> contains information about the songs artists.
* <b>users:</b> contains information about the users.
* <b>songs:</b> contains information about songs.
* <b>time:</b> contains information about the events time.

### 2. Project Structure
```
    .
    ├── etl.py                    # Manages the ETL process with pyspark.
    ├── dl.cfg                    # Contains all necessary credentials to connect to the songs and log data s3 bucket. 
    └── README.md
```
    
### 3. How to Run the Python Scripts

To run the script, execute this command
``` 
python etl.py 
```



