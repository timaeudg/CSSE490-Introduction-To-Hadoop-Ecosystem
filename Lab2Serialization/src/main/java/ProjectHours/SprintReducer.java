package ProjectHours;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Icarus on 9/20/2014.
 */
public class SprintReducer extends Reducer<SprintWritable, FloatWritable, SprintWritable, FloatWritable> {
    @Override
    protected void reduce(SprintWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("Key: " + key.toString());
        System.out.println("values: " + values.toString());
        float sum = 0;
        for (FloatWritable hours : values) {
            sum += hours.get();
        }
        context.write(key, new FloatWritable(sum));
    }
}
