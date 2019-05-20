JAR_NAME=$1
CLASS_NAME=$2
INPUT=$3
OPTION=$4

if [ $# -eq 0 ]
  then
    INPUT=NCDC-Weather.txt
    JAR_NAME=Hadoop
    CLASS_NAME=Main
    OPTION=
fi

cd $HADOOP_PREFIX
echo $JAR_NAME
echo $CLASS_NAME
echo $INPUT
echo $OPTION
bin/hadoop fs -mkdir mars
bin/hadoop fs -ls mars 
bin/hadoop fs -put $HADOOP_PREFIX/$INPUT mars
bin/hadoop fs -ls mars
bin/hadoop fs -rm -r -skipTrash $HADOOP_PREFIX/output
bin/hadoop jar $JAR_NAME.jar bdt.mars.hadoop.temp.average.$CLASS_NAME mars/$INPUT $HADOOP_PREFIX/output $OPTION
bin/hadoop fs -ls $HADOOP_PREFIX/output 
bin/hadoop fs -cat $HADOOP_PREFIX/output/part-r-00000


