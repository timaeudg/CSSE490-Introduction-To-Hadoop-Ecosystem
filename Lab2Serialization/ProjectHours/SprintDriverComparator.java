package ProjectHours;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SprintDriverComparator extends Configured implements Tool{


    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "Sprint Writable with Raw Comparator");

        job.setJarByClass(SprintDriver.class);
        job.setSortComparatorClass(SprintWritableComparator.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapOutputKeyClass(SprintWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(SprintMapper.class);
        job.setReducerClass(SprintReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SprintDriverComparator(), args);
        System.exit(exitCode);
    }
}
