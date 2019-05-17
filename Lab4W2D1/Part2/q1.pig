q1 = load 'MovieDataSet/users.txt' using PigStorage('|') as (rid:int,age:int,sex:chararray,occupation:chararray,id:int);
fltrd = FILTER q1 BY (sex=='M') AND (occupation=='lawyer');
prjctnd = FOREACH fltrd GENERATE sex;
preresult = GROUP prjctnd BY sex;
result = FOREACH preresult GENERATE COUNT($1) as count;
dump result;
rm output;
store result into 'output' using PigStorage(',');


