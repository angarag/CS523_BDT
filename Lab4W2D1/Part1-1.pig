input_lines = load 'input/countMyWords.txt' as (line:chararray);
wordsBag = FOREACH input_lines GENERATE TOKENIZE(line) as lineBag;
words = FOREACH wordsBag GENERATE flatten(lineBag);
temp = GROUP words BY token;
counts = FOREACH temp GENERATE group, COUNT(words) as count;
ordered = order counts by count desc;
store ordered into 'output' using PigStorage(',');

