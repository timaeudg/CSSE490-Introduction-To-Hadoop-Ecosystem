package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class GroupComparator extends WritableComparator {
	
	private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();
	
	public GroupComparator() {
		super(IntPair.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
	        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
	        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
	        return INT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
	      } catch (IOException e) {
	        throw new IllegalArgumentException(e);
	      } catch (NullPointerException e) {
	    	  throw new IllegalArgumentException(e);
	      }
	}

}
