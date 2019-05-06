q1 = load 'MovieDataSet/users.txt' using PigStorage('|') as (rid:int,age:int,sex:chararray,occupation:chararray,id:int);
//temp = LIMIT q1 100;
fltrd = FILTER q1 BY (sex=='M') AND (occupation=='lawyer');
prjctnd = FOREACH fltrd GENERATE sex;
preresult = GROUP prjctnd BY sex;
result = FOREACH preresult GENERATE COUNT($1) as count;
store result into 'output' using PigStorage(',');


