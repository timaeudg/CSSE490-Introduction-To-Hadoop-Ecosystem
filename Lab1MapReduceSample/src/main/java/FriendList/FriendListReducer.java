package FriendList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


public class FriendListReducer extends
		Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> commonFriends = new HashSet<String>();
        int counter = 0;
        for (Text friends : values) {
            List<String> friendList = Arrays.asList(friends.toString().split(" "));

            HashSet<String> filteredFriends = new HashSet<String>();
            for (String name : friendList) {
                if (counter == 0 || commonFriends.contains(name)) {
                    filteredFriends.add(name);
                }
            }
            commonFriends = filteredFriends;
            counter++;
        }
        if (counter < 2) {
            context.write(key, new Text(""));
        } else {
            context.write(key, new Text(Arrays.asList(commonFriends.toArray()).toString()));
        }
    }
}
