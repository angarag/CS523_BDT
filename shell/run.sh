#Run Producer program
#After exporting it as a runnable jar, run the followings (it is assumed that the jar file is exported to Desktop folder):
cd ~/Desktop
spark-submit --class bdt.mars.project.v1.Producer --master yarn CS523_986689.jar
sleep 3s

#Run Consumer program
spark-submit --class bdt.mars.project.v1.Consumer --master yarn CS523_986689.jar
sleep 300s

#Run SparkSQL program
spark-submit --class bdt.mars.project.v1.HiveUtil --master yarn CS523_986689.jar