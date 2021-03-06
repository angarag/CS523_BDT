q1 = load 'MovieDataSet/users.txt' using PigStorage('|') as (rid:int,age:int,sex:chararray,occupation:chararray,zip:int);
fltrd = FILTER q1 BY (sex=='M') AND (occupation=='lawyer');
prjctnd = FOREACH fltrd GENERATE age,rid;
ordered = ORDER prjctnd BY age DESC;
preresult = LIMIT ordered 1;
result = FOREACH preresult GENERATE rid;
dump result;
rm output;
store result into 'output' using PigStorage(',');
