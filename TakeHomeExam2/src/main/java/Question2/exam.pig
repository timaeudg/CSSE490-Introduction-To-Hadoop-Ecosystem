register '/tmp/Exams/timaeudgUDF.jar';
gradeRecords = LOAD '$grades' using PigStorage(',') AS (fname:chararray, lname:chararray, courseTaken:chararray, grade:float);
courseRecords = LOAD '$courses' using PigStorage(',') AS (courseID:chararray, courseName:chararray);
filteredGrades = FILTER gradeRecords by grade<=90;
formattedGrades = FOREACH filteredGrades GENERATE Question2.ConcatWithSpace(fname, lname) as name, courseTaken as courseTaken, Question2.ConvertToLetterGrade(grade) as letterGrade;
joined = JOIN formattedGrades BY courseTaken, courseRecords BY courseID;
finalOutput = FOREACH joined GENERATE name, courseID, courseName, letterGrade;
STORE finalOutput into '$outputDir/$username' using PigStorage('\t');
