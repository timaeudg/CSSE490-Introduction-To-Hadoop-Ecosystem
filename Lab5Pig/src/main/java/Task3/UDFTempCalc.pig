register '$jar';
records = LOAD '$input' using PigStorage('\t') AS (year:chararray, temperature:int, quality:int);
frecords = FILTER records BY Task3.FilterBadTemps(quality);
grecords = GROUP frecords by year;
avgTemp = FOREACH grecords GENERATE group, MIN(frecords.temperature) as MinTemp, MAX(frecords.temperature) as MaxTemp, AVG(frecords.temperature) as AvgTemp;
STORE avgTemp into '$output' using PigStorage(',');
