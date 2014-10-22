package ProjectHours;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SprintWritable implements WritableComparable<SprintWritable> {

    private Text lastName;
    private Text firstName;
    private IntWritable sprint;

    public SprintWritable() {
        lastName = new Text();
        firstName = new Text();
        sprint = new IntWritable();
    }

    public SprintWritable(Text first, Text last, IntWritable sprint) {
        lastName = last;
        firstName = first;
        this.sprint = sprint;
    }

    @Override
    public String toString() {
        return firstName + "\t" + lastName + "\t" + sprint;
    }

    public void write(DataOutput out) throws IOException {
        firstName.write(out);
        lastName.write(out);
        sprint.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        firstName.readFields(in);
        lastName.readFields(in);
        sprint.readFields(in);
    }

    public int compareTo(SprintWritable o) {
        int cmp = firstName.compareTo(o.firstName);
        if (cmp != 0) {
            return cmp;
        }
        cmp = lastName.compareTo(o.lastName);
        if (cmp != 0) {
            return cmp;
        }
        return sprint.compareTo(o.sprint);
    }
}
