package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinCoursesMapper extends
        Mapper<LongWritable, Text, TextPair, TextPair> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String[] records = value.toString().split(",");
            TextPair pair = new TextPair(records[0], "");
            context.write(pair, new TextPair(records[1], ""));
        } catch (Exception e) {
            throw new IllegalArgumentException("Sprint Mapper had an exception");
        }
    }

}
