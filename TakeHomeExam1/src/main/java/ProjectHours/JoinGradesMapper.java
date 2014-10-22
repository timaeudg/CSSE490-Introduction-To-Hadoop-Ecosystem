package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinGradesMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String[] values = value.toString().split(",");
            String name = values[0];
            String classID = values[1];
            float score = Float.parseFloat(values[2]);

            context.write(new TextPair(classID, "secondKey"), 
                    new TextPair(name, Float.toString(score)));
        } catch (Exception e) {
            throw new IllegalArgumentException("Work Mapper had an exception");
        }
    }

}
