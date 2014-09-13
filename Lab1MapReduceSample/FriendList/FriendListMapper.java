package FriendList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FriendListMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<String> splitNames = Arrays.asList(line.split(","));
        String person = splitNames.get(0);
        List<String> friendList = splitNames.subList(1, splitNames.size());

        for (int i = 0; i < friendList.size(); i++) {
            String friend = friendList.get(i);
            List<String> reducedFriendList = new ArrayList<String>(friendList);
            reducedFriendList.remove(i);
            List<String> pair = new ArrayList<String>();
            pair.add(person);
            pair.add(friend);
            Collections.sort(pair);

            context.write(new Text(pair.get(0) + " " + pair.get(1)), new Text(StringUtils.join(reducedFriendList, " ")));
        }
    }
}
