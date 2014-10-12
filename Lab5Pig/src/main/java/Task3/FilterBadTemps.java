package Task3;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class FilterBadTemps extends FilterFunc {
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        try {
            Integer quality = (Integer) input.get(0);
            if (quality != 0 && quality != 1) {
                return false;
            }
            return true;
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}