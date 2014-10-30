package Question2;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ConcatWithSpace extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            String first = (String) input.get(0);
            String last = (String) input.get(1);
            return first + " " + last;
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}