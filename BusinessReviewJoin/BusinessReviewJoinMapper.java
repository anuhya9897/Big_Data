import java.io.IOException;
import org.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessReviewJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text output_key = new Text();
    private Text output_value = new Text();
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Boolean isBusinessRecord = false;
        String reviewid = "";
        JSONObject line = new JSONObject(value.toString());
        try {
            reviewid = line.getString("review_id");
        }
        catch (Exception e) {
            isBusinessRecord = true;
        }
        output_key.set(line.getString("business_id"));
        if (isBusinessRecord) {
            String output = line.getString("name") + "," +
                    line.getString("address") + "," +
                    line.getDouble("stars") + "," +
                    line.getInt("review_count");
            output_value.set(output);
        }
        else {
            String output = line.getString("user_id") + "," +
                    line.getInt("stars") + "," +
                    line.getString("text");
            output_value.set(output);
        }
        context.write(output_key, output_value);
    }

}
