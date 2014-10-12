register '$jar';
records = LOAD '$input';
flattenedRecord = FOREACH records GENERATE FLATTEN(TOKENIZE((chararray)$0)) as word;
groupedWords = GROUP flattenedRecord by word;
wordCounts = FOREACH groupedWords GENERATE Task2.Upper(group), COUNT(flattenedRecord);
STORE wordCounts into '$output' using PigStorage(',');
