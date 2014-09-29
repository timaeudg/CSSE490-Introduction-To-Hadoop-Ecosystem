package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSprintMapper extends Mapper<LongWritable, Text, IntPair, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		try {
		String[] records = value.toString().split(",");
		IntPair pair = new IntPair(Integer.parseInt(records[0]), 2);
		System.out.println("SprintMapper\nIntPairFirstValue: " + pair.getFirst().get() + "\nIntPairSecondValue: " + pair.getSecond().get());
		System.out.println("SprintMapper\nTextValue: " + records[1]);
		context.write(pair, new Text(records[1]));
		} catch (Exception e) {
			throw new IllegalArgumentException("Sprint Mapper had an exception");
		}
	}

}
