package ProjectHours;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<IntPair, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntPair arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		Iterator<Text> iter = arg1.iterator();
		System.out.println("in reducer");
		Text sprint = new Text(iter.next());
		System.out.println("sprint: " + sprint.toString());
		while(iter.hasNext()) {
			Text work = iter.next();
			System.out.println("work: " + work.toString());
			Text outValue = new Text(sprint.toString() + "\t" + work.toString());
			System.out.println("output value: " + arg0.getFirst().toString());
			arg2.write(arg0.getFirst(), outValue);
		}
	}

}
