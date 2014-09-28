package CustomInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomWordCountDriver extends Configured implements Tool {
	
	enum WordCount {
		EqualToTwo,
		LessThanTwo,
		GreaterThanTwo
	}

	public int run(String[] args) throws Exception {

		if(args.length !=3 ){
			System.err.println("Usage: Max Temperature <input path> <outputh path> <word to search for>");
			System.exit(-1);
		}
		
		String stringToSearch = args[2];
		
		Configuration conf = getConf();
		conf.set("stringToSearch", stringToSearch);
		
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		job.setInputFormatClass(CustomWordCountInputFormat.class);
		
		CustomWordCountInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(CustomWordCountMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
		Counters counter = job.getCounters();
		long greater = counter.findCounter(CustomWordCountDriver.WordCount.GreaterThanTwo).getValue();
		long less = counter.findCounter(CustomWordCountDriver.WordCount.LessThanTwo).getValue();
		long equal = counter.findCounter(CustomWordCountDriver.WordCount.EqualToTwo).getValue();
		long bytes = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_BYTES").getValue();
		
		System.out.println("# of files with MORE than 2 occurences of \"" + stringToSearch + "\": " + greater);
		System.out.println("# of files with LESS than 2 occurences of \"" + stringToSearch + "\": " + less);
		System.out.println("# of files with EQUAL to 2 occurences of \"" + stringToSearch + "\": " + equal);
		System.out.println("# of output bytes from the Mapper: " + bytes);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CustomWordCountDriver(),args);
		System.exit(exitCode);
	}
	
}
