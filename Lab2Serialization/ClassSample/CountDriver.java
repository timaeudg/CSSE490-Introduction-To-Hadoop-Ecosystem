package ClassSample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Created by Icarus on 9/20/2014.
 */
public class CountDriver extends Configured implements Tool{

    public int run(String[] strings) throws Exception {
        if (strings.length != 3) {
            System.err.println("Usage: CountDriver <input path> <output path>");
            System.err.println("Got: " + Arrays.toString(strings));
            return -1;
        }
        Configuration conf = getConf();
        Job job = new Job(conf, "Custom Writable example");

        job.setJarByClass(CountDriver.class);

        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(TextPairMapper.class);
        job.setReducerClass(TextPairReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CountDriver(), args);
        System.exit(exitCode);
    }
}
