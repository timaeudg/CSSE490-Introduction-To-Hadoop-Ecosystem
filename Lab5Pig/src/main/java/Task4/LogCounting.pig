%declare DATE `date +%F`;
register '$jar';
DEFINE CurrentTime org.apache.pig.builtin.CurrentTime();
DEFINE GetDay org.apache.pig.builtin.GetDay();
DEFINE GetHour org.apache.pig.builtin.GetHour();
DEFINE GetMonth org.apache.pig.builtin.GetMonth();
DEFINE GetYear org.apache.pig.builtin.GetYear();
records = LOAD '$input' using PigStorage('\t') AS (date:chararray, time:chararray, location:chararray, bytes:int, ip:chararray, method:chararray, host:chararray, uriStem:chararray, status:chararray, referer:chararray, userAgent:chararray, uriQuery:chararray, cookie:chararray, edgeResultType:chararray, requestId:chararray);
filteredColumns = FOREACH records GENERATE STRSPLIT(uriStem, '/', 4).$1 AS isBlog, STRSPLIT(uriStem, '/', 4).$2 AS blog, edgeResultType;
validRows = FILTER filteredColumns BY (chararray)isBlog == (chararray)'blogs';
hitRows = FILTER validRows BY edgeResultType == 'Hit';
groupedHits = GROUP hitRows by blog;
groupedRows = GROUP validRows by blog;
countedHits = FOREACH groupedHits GENERATE group as blog, COUNT(hitRows.blog) as hits;
countedRows = FOREACH groupedRows GENERATE group as blog, COUNT(validRows.blog) as total;
joined = JOIN countedHits BY blog, countedRows BY blog;
ratioReady = FOREACH joined GENERATE $0 as blog, $1 as hits, $3 as total;
hitsRatio = FOREACH ratioReady GENERATE blog, Task4.GetHitRatio(hits, total), Task4.GetHitRatio(total-hits, total), GetYear(CurrentTime()), GetMonth(CurrentTime()), GetDay(CurrentTime()), GetHour(CurrentTime());
STORE hitsRatio into '$output/$DATE' using PigStorage('\t');

