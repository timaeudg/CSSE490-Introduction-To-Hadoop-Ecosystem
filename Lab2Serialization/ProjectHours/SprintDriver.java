package ProjectHours;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class SprintDriver  extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.println("Usage: SprintDriver <input path> <output path>");
            System.err.println("Got: " + Arrays.toString(strings));
            return -1;
        }
        Configuration conf = getConf();
        Job job = new Job(conf, "Sprint hours");

        job.setJarByClass(SprintDriver.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapOutputKeyClass(SprintWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setMapperClass(SprintMapper.class);
        job.setReducerClass(SprintReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SprintDriver(), args);
        System.exit(exitCode);
    }
}