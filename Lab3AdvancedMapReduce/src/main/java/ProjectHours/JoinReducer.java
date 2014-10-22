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
		Text sprint = new Text(iter.next());
		while(iter.hasNext()) {
			Text work = iter.next();
			Text outValue = new Text(sprint.toString() + "\t" + work.toString());
			arg2.write(arg0.getFirst(), outValue);
		}
	}

}
