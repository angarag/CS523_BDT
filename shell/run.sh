#After exporting it as a runnable jar, run the followings:
spark-submit --class bdt.mars.project.v1.Producer --master yarn CS523_986689.jar
sleep 3s
mkdir input
mkdir conf
touch conf/app.properties
URL=$Elastic_URL
PASSWORD=$Elastic_password
echo "url=$URL" >> conf/app.properties
echo "password=$PASSWORD" >> conf/app.properties
spark-submit --class bdt.mars.project.v1.Consumer --master yarn CS523_986689.jar
sleep 300s
spark-submit --class bdt.mars.project.v1.HiveUtil --master yarn CS523_986689.jar