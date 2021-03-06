REGISTER /usr/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage('\t','NO_MULTILINE','UNIX','WRITE_OUTPUT_HEADER'); 
temp = load 'MovieDataSet/movies.csv' using CSVLoader() as (rid:int,title:chararray,genres:chararray);
fltrd = FILTER temp BY (genres matches '.*Adventure.*');
prjctd = FOREACH fltrd GENERATE $0,$1;

rating_input = load 'MovieDataSet/rating.txt' as (userid:int, id:int, rating:int, time:chararray);
rating_fltrd = FILTER rating_input BY (rating==5);
rating = FOREACH rating_fltrd GENERATE $1;

joined = COGROUP prjctd BY $0 INNER, rating BY $0;
joined_fltrd = FILTER joined BY SIZE(TOTUPLE(*))>2;
rdcd = FOREACH joined_fltrd GENERATE $0, FLATTEN($1.$1) as title, COUNT($2.$0) as count;
formatted = FOREACH rdcd GENERATE $0 as movieId, 'Adventure' as genres, '5' as rating, $1 as title;
pre_result = ORDER formatted BY $0;
result = LIMIT pre_result 20;
STORE result INTO 'output' USING CSVExcelStorage;
dump result;
