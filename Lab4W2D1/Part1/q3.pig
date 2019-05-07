q3 = load 'MovieDataSet/movies.csv' using PigStorage(',') as (rid:int,age:int,sex:chararray,occupation:chararray,id:int);
//temp = LIMIT q1 100;
fltrd = FILTER q3 BY (sex=='M') AND (occupation=='lawyer');
prjctnd = FOREACH fltrd GENERATE age,id;
ordered = ORDER prjctnd BY age DESC;
preresult = LIMIT ordered 1;
result = FOREACH preresult GENERATE id;
store result into 'output' using PigStorage(',');