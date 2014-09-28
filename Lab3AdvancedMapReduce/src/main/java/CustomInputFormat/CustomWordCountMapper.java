package CustomInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CustomWordCountMapper extends
		Mapper<Text, IntWritable, Text, IntWritable> {

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	@Override
	protected void map(Text key, IntWritable value,
			Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
		if (value.equals(2)) {
			context.getCounter(CustomWordCountDriver.WordCount.EqualToTwo).increment(1);
		} else if (value.compareTo(new IntWritable(2)) < 0) {
			context.getCounter(CustomWordCountDriver.WordCount.LessThanTwo).increment(1);
		} else {
			context.getCounter(CustomWordCountDriver.WordCount.GreaterThanTwo).increment(1);
		}
	}

}
