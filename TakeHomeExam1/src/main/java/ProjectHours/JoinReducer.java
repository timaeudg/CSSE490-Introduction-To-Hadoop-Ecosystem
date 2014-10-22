package ProjectHours;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextPair, TextPair, Text, Text> {

    @Override
    protected void reduce(TextPair arg0, Iterable<TextPair> arg1, Context arg2)
            throws IOException, InterruptedException {
        Iterator<TextPair> iter = arg1.iterator();
        TextPair courseInfo = new TextPair(iter.next());
        String courseName = courseInfo.getFirst().toString();
        while (iter.hasNext()) {
            TextPair gradeInfo = iter.next();
            String courseID = arg0.getFirst().toString();
            String studentName = gradeInfo.getFirst().toString();
            String score = gradeInfo.getLast().toString();
            Text outValue = new Text(courseID +
                    "\t" + courseName + "\t" + 
                    score);
            arg2.write(new Text(studentName), outValue);
        }
    }

}
