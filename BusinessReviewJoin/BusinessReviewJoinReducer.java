import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessReviewJoinReducer extends Reducer<Text, Text, Text, Text> {
    Text outputkey = new Text();
    ArrayList<Text> outputvalues = new ArrayList<>();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] v = value.toString().split(",");
            if (v.length == 4) {
                outputkey.set(value);
            }
            else {
                outputvalues.add(value);
            }
        }
        for (Text ov : outputvalues) {
            context.write(outputkey, ov);
        }
    }
}
