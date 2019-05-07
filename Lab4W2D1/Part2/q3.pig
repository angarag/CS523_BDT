temp = load 'MovieDataSet/movies.csv' using PigStorage(',') as (rid:int,title:chararray,genres:tuple({tuple...}));
q3 = LIMIT temp 100;
fltrd = FILTER q3 BY (sex=='M') AND (occupation=='lawyer');
prjctnd = FOREACH fltrd GENERATE age,id;
ordered = ORDER prjctnd BY age DESC;
preresult = LIMIT ordered 1;
result = FOREACH preresult GENERATE id;
rm output;
store result into 'output' using PigStorage(',');
