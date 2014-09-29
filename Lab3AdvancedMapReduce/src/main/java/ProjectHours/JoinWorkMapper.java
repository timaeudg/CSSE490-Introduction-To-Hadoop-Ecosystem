package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinWorkMapper extends Mapper<LongWritable, Text, IntPair, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		try {
		String[] values = value.toString().split(",");
		String name = values[0] + " " + values[1];
		int sprint = Integer.parseInt(values[2]);
		
		IntPair pair = new IntPair(sprint, 100);
		System.out.println("WorkMapper\nFirstValue: " + pair.getFirst().get() + "\nSecondValue: " + pair.getSecond().get());
		System.out.println("WorkMapper\nTextValue: " + name);
		context.write(pair, new Text(name));
		} catch (Exception e) {
			throw new IllegalArgumentException("Work Mapper had an exception");
		}
	}

}
