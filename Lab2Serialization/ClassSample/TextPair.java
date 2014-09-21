package ClassSample;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Icarus on 9/20/2014.
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text firstName;
    private Text lastName;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String last) {
        set(new Text(first), new Text(last));
    }

    public TextPair(Text first, Text last) {
        set(first, last);
    }

    public void set(Text first, Text last) {
        firstName = first;
        lastName = last;
    }

    public Text getFirst() {
        return firstName;
    }

    public Text getLast() {
        return lastName;
    }

    public int compareTo(TextPair o) {
        int cmp = firstName.compareTo(o.firstName);
        if (cmp != 0) {
            return cmp;
        }
        return lastName.compareTo(o.lastName);
    }

    public void write(DataOutput dataOutput) throws IOException {
        firstName.write(dataOutput);
        lastName.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        firstName.readFields(dataInput);
        lastName.readFields(dataInput);
    }

    @Override
    public String toString() {
        return firstName + "\t" + lastName;
    }
}
