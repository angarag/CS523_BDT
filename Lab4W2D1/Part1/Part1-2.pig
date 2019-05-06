users = load 'input/users.csv' using PigStorage(',') as (user:chararray,age:int);
usersFiltered = FILTER users by (age>17) AND (age<26);
pages = load 'input/pages.csv' using PigStorage(',') as (user:chararray,url:chararray);
joined = JOIN usersFiltered BY $0, pages BY $0;
projected = FOREACH joined GENERATE $3;
grouped = GROUP projected BY $0;
summed = FOREACH grouped GENERATE group, COUNT($1);
sorted = ORDER summed BY $1 DESC;
top5 = LIMIT sorted 5;
store top5 into 'output' using PigStorage(',');


