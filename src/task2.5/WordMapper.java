package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text stateText = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if (fields.length > 8) {
            String stateField = fields[7].trim(); // Extract 8th column

            if (!stateField.isEmpty()) {
                String[] stateList = stateField.split(",");

                for (String state : stateList) {
                    stateText.set(state.trim());
                    context.write(stateText, one);
                }
            }
        }
    }
}
