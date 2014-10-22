package ProjectHours;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SprintMapper extends Mapper<LongWritable, Text, SprintWritable, FloatWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String firstname = tokens[0];
        String lastname = tokens[1];
        int sprint = Integer.parseInt(tokens[2]);
        float hours = Float.parseFloat(tokens[3]);

        SprintWritable outputKey = new SprintWritable(new Text(firstname), new Text(lastname), new IntWritable(sprint));
        context.write(outputKey, new FloatWritable(hours));
    }
}
