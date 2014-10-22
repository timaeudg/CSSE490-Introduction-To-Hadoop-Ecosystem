package Task2;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Strip extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        return new Text(s.toString().replaceAll("[(,)]", ""));
    }

}