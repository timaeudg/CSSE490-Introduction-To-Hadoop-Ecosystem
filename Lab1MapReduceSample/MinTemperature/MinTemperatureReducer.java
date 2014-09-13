import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MinTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int minValue = Integer.MAX_VALUE;
        for (IntWritable data : values) {
            minValue = Math.min(minValue, data.get());
        }
        context.write(key, new IntWritable(minValue));
    }
}
