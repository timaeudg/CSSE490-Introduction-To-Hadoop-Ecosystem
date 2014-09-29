package ProjectHours;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JoinDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if(args.length != 3) {
			System.err.println("Usage: Join <work details path> <sprint details path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		
		Path workDetails = new Path(args[0]);
		Path sprintDetails = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, workDetails,
				TextInputFormat.class, JoinWorkMapper.class);
		MultipleInputs.addInputPath(job, sprintDetails,
				TextInputFormat.class, JoinSprintMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(JoinReducer.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinDriver(), args);
		System.exit(exitCode);
	}

}
