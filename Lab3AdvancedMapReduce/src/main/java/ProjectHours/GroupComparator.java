package ProjectHours;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class GroupComparator extends WritableComparator {
	
	
	public GroupComparator() {
		super(IntPair.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
	        int firstL1 = readInt(b1, s1);
	        int firstL2 = readInt(b2, s2);
	        return Integer.compare(firstL1, firstL2);
	      } catch (NullPointerException e) {
	    	  throw new IllegalArgumentException(e);
	      }
	}

}
