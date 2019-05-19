#After exporting it as a runnable jar, run the followings:
spark-submit --class bdt.mars.project.v1.Producer --master yarn CS523_986689.jar
sleep 3s
mkdir input
spark-submit --class bdt.mars.project.v1.Consumer --master yarn CS523_986689.jar
sleep 30s
spark-submit --class bdt.mars.project.v1.HiveUtil --master yarn CS523_986689.jar