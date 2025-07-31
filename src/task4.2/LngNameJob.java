package stubs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngNameJob {

    // Mapper class: extracts individual names from input
    public static class NameExtractorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text nameText = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t");

            if (fields.length > 1) {
                String[] nameParts = fields[1].split("/");

                for (String part : nameParts) {
                    String cleanName = part.trim();

                    if (!cleanName.isEmpty()) {
                        nameText.set(cleanName);
                        context.write(nameText, NullWritable.get());
                    }
                }
            }
        }
    }

    // Reducer class: filters out duplicate names
    public static class DeduplicationReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text name, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(name, NullWritable.get());
        }
    }

    // Driver method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LngNameJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Extract Unique Language Names");

        job.setJarByClass(LngNameJob.class);
        job.setMapperClass(NameExtractorMapper.class);
        job.setReducerClass(DeduplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean completed = job.waitForCompletion(true);
        System.exit(completed ? 0 : 1);
    }
}