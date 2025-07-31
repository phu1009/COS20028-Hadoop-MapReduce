package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngThlJob {

    // Mapper: extracts individual thl values from column 4 (index 3)
    public static class ThlFieldMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text thlText = new Text();

        @Override
        protected void map(LongWritable lineOffset, Text inputLine, Context context)
                throws IOException, InterruptedException {

            String[] columns = inputLine.toString().split("\t");

            if (columns.length > 3) {
                String[] thlItems = columns[3].split("/");

                for (String rawThl : thlItems) {
                    String cleanedThl = rawThl.trim();

                    if (!cleanedThl.isEmpty()) {
                        thlText.set(cleanedThl);
                        context.write(thlText, NullWritable.get());
                    }
                }
            }
        }
    }

    // Reducer: outputs distinct thl entries
    public static class UniqueThlReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text thlEntry, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(thlEntry, NullWritable.get());
        }
    }

    // Job configuration and execution
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LngThlJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Extract Unique THL Entries");

        job.setJarByClass(LngThlJob.class);
        job.setMapperClass(ThlFieldMapper.class);
        job.setReducerClass(UniqueThlReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobSuccess = job.waitForCompletion(true);
        System.exit(jobSuccess ? 0 : 1);
    }
}
