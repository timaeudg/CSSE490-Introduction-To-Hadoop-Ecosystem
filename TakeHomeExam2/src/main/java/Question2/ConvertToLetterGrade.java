package Question2;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ConvertToLetterGrade extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            Float grade = (Float) input.get(0);
            if (grade <= 60.0) {
                return "F";
            } else if (grade <= 70.0) {
                return "D";
            } else if (grade <= 80.0) {
                return "C";
            } else if (grade < 90.0) {
                return "B";
            } else {
                return "A";
            }
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}