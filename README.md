# CS523_BDT - Live election result 
This is the project Angarag Batjargal did as his final project for the Big Data Technology course taught in May block, 2019.   

##Project description 

This program is to show a Live election result of user votes. Users are to vote for their favorite characters from the Game of Thrones season.  

##Program environment 
###The followings are used in the program: 

-Java 1.8 

-Kafka 2.2.0 

-Spark 2.2.2 

-HBase 

-Hive 

-ElasticSearch & Kibana on the cloud 

Please see the pom file for the version details. 

Project parts & the breakdown structure of the program: 

Part number 

The related sub-programs & Interfaces 

1 – Spark Streaming 

Consumer & Producer 

2 – SparkSQL with HBase/Hive 

HbaseUtil, HiveUtil 

3 - GUI 

ElasticSearchUtil, ElasticSearch.co & Kibana 

4 - Kafka 

Consumer, Producer 

Page Break
 

Source code 

It is a Maven project. So once it is added into your IDE, you can export it as a runnable jar. This project consists of the following Java classes: 

-Consumer (Spark Streaming as a Kafka consumer) 

-Producer (Kafka message producer, emits random messages) 

-ElasticSearchUtil (ElasticSearch client to save record into the cloud service of ElasticSearch.co) 

-HBaseUtil (HBase utility to save kafka message record to HBase database) 

-HiveUtil (SparkSQL client to query HBase table) 

-Names (the most common 100 first names in the USA) 

First let me define the parameters I used: 

-VoteFor – Election candidate, one of the random names of “Arya, Sansa, Jon, Daenerys, Cersei” defined in Producer class 

-User – One of the random user from the Names class 

-Timestamp – the timestamp when user votes for candidate 

-Count – it is by default 1. the number of points/votes user votes for his/her favourite candidate 

I will explain the Java classes one by one: 

#Producer program 

It emits a message in the following format every 0.5 second until it receives termination request. 

```Key, Value: “voteFor”, “user,timestamp” 
```
#Consumer program 

Consumer class listens to the emitted Kafka messages with Spark Streaming every 0.5 second 

--Group Kafka messages by the candidate with window size 5 minutes 

--Show the grouped message in the console 

--Save message to Elastic search 

--Save message to HBase 

--Save log to local file 

-HBaseUtil 

It is used in the Consumer program to store Kafka message in HBase table. 

#HiveUtil 

It is a standalone sub-program written with SparkSQL. It queries the HBase table via Hive to show the number of votes for every candidate. This is for admin purpose. 

#ElasticSearchUtil 

It is used in Consumer program to save vote record on the cloud service of ElasticSearch. 

How to run the program 

##Prerequisites:  

Create index “election” & type “_doc” on Elastic Search & Create index on Kibana 

Refer to the elasticSchema.json file in the source folder. 

Create configuration file named app.properties under conf folder with the following parameters: (Note that “/election/_doc/ is appended to the ElasticSearch url” 
```
url=$elasticsearch_url/election/_doc/ 
password= $elasticsearch_password 
```
-Create input directory where you run the program 

-Create Kafka topic named “election” 

-Create election table in HBase with column family “vote_details” 

-Create Hive-Hbase mapping by using the hive script in the Hive-HBase integration text file 

--Refer to hive-Hbase_integration.txt file 
```
CREATE EXTERNAL TABLE election(id STRING, voteFor STRING, user STRING, count STRING, date TIMESTAMP) ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,vote_details:voteFor,vote_details:user,vote_details:count,vote_details:timestamp") TBLPROPERTIES("hbase.table.name"="election"); 
```

-HBase: election:vote_details(key,voteFor,user,count,timestamp) 

-=> Hive:election(id,voteFor,user,count,date) 

Copy hive-site.xml file (located in /usr/lib/hive/conf) to the src folder of the program so that the program can find hive settings 

1) First we need to run the Kafka by running the Kafka shell named runKafka.sh 

2) Now we can produce Kafka messages on the topic “election” by running the Producer class 

3) We can run the following shell scripts to start Producer, Consumer and HiveUtil: 

Shell scrip1: /shell/prepare_environment.sh 

Shell script2: /shell/run.sh 

#After exporting it as a runnable jar, run the followings:,  
```
spark-submit --class bdt.mars.project.v1.Producer --master yarn CS523_986689.jar 
sleep 3s 
mkdir input 
spark-submit --class bdt.mars.project.v1.Consumer --master yarn CS523_986689.jar 
sleep 30s 
spark-submit --class bdt.mars.project.v1.HiveUtil --master yarn CS523_986689.jar 
```
It is assumed that the program is exported as a single runnable jar named “CS523_986689.jar”.  

#How to terminate the program: 

-Terminate Producer program 

-Terminate Consumer program 

-Stop Kafka shell 

-Stop refresh rate on ElasticSearch – Kibana dashboard 

#Commands 

The following shell scripts are located in the src/bdt/mars/project/shell. 

#Monitoring 

We have many options to monitor the Election result as below: 

See Spark Streaming log from the Consumer class 

Monitor the local file “input/election_result.txt” on each node (create the directory first) 

Monitor ElasticSearch – Kibana live dashboard 

Run HiveUtil to monitor HBase records and grouped message per candidate 

#HBase: 

Run “hbase shell” command and enter “scan ‘election’” 

#Troubleshooting 

*Hive permission issue on /tmp/hive directory  

--> Enable hive directory: ```sudo chmod 777 /tmp/hive ```

*Address already in use issue: 

-> Kill the previous process: ```sudo fuser –k –n tlp $port ```

*To check Hive, HBase connection: 

-Run HiveUtil class (it will create election table) 

-Run HBaseUtil class (it will create election table) 

*Target host is null when sending HTTP request to ElasticSearch 

--> Check if the URL is correct in conf/app.properties file 

*Creating Hbase table failed 

--> run the restartZooKeeper shell script to restart ZooKeeper & HBaser services. 

#Useful guides for preparing environment 

-How to install Java  8 on Centos 6 
```
sudo yum install java-1.8.0-openjdk –y
sudo vi /etc/profile  //Change the JAVA_HOME 
source /etc/profile //Or re-login to make the new JAVA_HOME effective 
```
#How to configure Github SSH 
```
ssh-keygen -t rsa -b 4096 -C your_email@example.com 
ssh-add ~/.ssh/id_rsa 
clip < ~/.ssh/id_rsa.pub 
```

*Elastic credentials in conf/app.properties: 
```
password=ILhRhmggITmEyuu8na8vKOsN 
url=https://c8d08836a88346e6894f02cc722ed09a.us-central1.gcp.cloud.es.io:9243/election/_doc 
```
You can refer to the shell scripts in the shell directory which automates all these preparations. 
