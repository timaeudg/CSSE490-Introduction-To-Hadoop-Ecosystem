package ProjectHours;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private IntWritable first;
	private IntWritable second;
	
	public IntPair() {
		set(new IntWritable(), new IntWritable());
	}
	
	public IntPair(int first, int second) {
		set(new IntWritable(first), new IntWritable(second));
	}
	
	public IntWritable getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	public IntPair(IntWritable first, IntWritable second) {
		set(first, second);
	}
	
	private void set(IntWritable first, IntWritable second) {
		if (first == null || second == null) {
			throw new IllegalArgumentException("IntPair got at least one null");
		}
		this.first = first;
		this.second = second;
	}
	
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public int compareTo(IntPair arg0) {
		int cmp = first.compareTo(arg0.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(arg0.second);
	}
	
	@Override
	public int hashCode() {
		return this.first.get()*this.second.get();
		
	}
	
	@Override
	public boolean equals(Object obj) {
		IntPair other = null;
		if (obj instanceof IntPair) {
			other = (IntPair) obj;
			return other.first.equals(this.first) && other.second.equals(this.second);
		} else {
			return false;
		}
	}

}
