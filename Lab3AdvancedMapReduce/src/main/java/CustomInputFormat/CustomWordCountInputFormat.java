package CustomInputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomWordCountInputFormat extends FileInputFormat<Text, IntWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<Text, IntWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		CustomWordCountRecordReader reader = new CustomWordCountRecordReader();
		reader.initialize(arg0, arg1);
		return reader;
	}
	

}
