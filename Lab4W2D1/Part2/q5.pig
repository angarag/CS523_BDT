REGISTER /usr/lib/pig/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 
temp = load 'MovieDataSet/movies.csv' using CSVExcelStorage() as (rid:int,title:chararray,genres:chararray);
fltrd = FILTER temp BY (genres matches '.*Adventure.*');
prjctd = FOREACH fltrd GENERATE $0,$1;

rating_input = load 'MovieDataSet/rating.txt' as (userid:int, id:int, rating:int, time:chararray);
rating_fltrd = FILTER rating_input BY (rating==5);
rating = FOREACH rating_fltrd GENERATE $1;

joined = COGROUP prjctd BY $0 INNER, rating BY $0;
joined_fltrd = FILTER joined BY SIZE(TOTUPLE(*))>2;
rdcd = FOREACH joined_fltrd GENERATE $0, FLATTEN($1.$1) as title, COUNT($2.$0) as count;
formatted = FOREACH rdcd GENERATE $0 as movieId;
pre_result = ORDER formatted BY $0;
q4_result = LIMIT pre_result 20;

q1 = load 'MovieDataSet/users.txt' using PigStorage('|') as (rid:int,age:int,sex:chararray,occupation:chararray,id:int);
fltrd = FILTER q1 BY (sex=='M') AND (occupation=='programmer');
user_ids = FOREACH fltrd GENERATE $0;
joined = JOIN rating_input BY $0, user_ids BY $0;
programmer_ratings = FOREACH joined GENERATE $0,$1,$2;
programmer_ratings_for_top20 = JOIN programmer_ratings BY $1, q4_result BY $0;
result_grouped = GROUP programmer_ratings_for_top20 ALL;
result = FOREACH result_grouped GENERATE COUNT($1);
store result into 'output' using PigStorage(',');
dump result;



