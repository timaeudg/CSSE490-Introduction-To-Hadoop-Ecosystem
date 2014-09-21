package ClassSample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Icarus on 9/20/2014.
 */
public class TextPairMapper  extends Mapper<LongWritable, Text, TextPair, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String firstName = tokens[0];
        String lastName = tokens[1];

        TextPair textPair = new TextPair(firstName, lastName);
        context.write(textPair, new IntWritable(1));
    }
}
