package Task4;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetHitRatio extends EvalFunc<Float> {
    public Float exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            Long hits = (Long) input.get(0);
            Long total = (Long) input.get(1);
            
            if(total == 0) {
                return null;
            }
            return new Float(hits)/new Float(total);
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}