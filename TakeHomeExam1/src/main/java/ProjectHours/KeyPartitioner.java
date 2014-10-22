package ProjectHours;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<IntPair, Text> {

	@Override
	public int getPartition(IntPair arg0, Text arg1, int arg2) {
		try {
		return (arg0.getFirst().get() & Integer.MAX_VALUE) % arg2;
		} catch (Exception e) {
			throw new IllegalArgumentException("Partitioner Threw an exception");
		}
	}

}
