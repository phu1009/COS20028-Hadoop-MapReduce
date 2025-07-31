package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngSynonymJob {

    // Mapper: extracts individual synonyms from the 3rd column
    public static class SynonymExtractorMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text synonymText = new Text();

        @Override
        protected void map(LongWritable offset, Text inputLine, Context context)
                throws IOException, InterruptedException {
            
            String[] columns = inputLine.toString().split("\t");

            if (columns.length > 2) {
                String[] synonymList = columns[2].split(",");

                for (String rawSynonym : synonymList) {
                    String cleaned = rawSynonym.trim();
                    if (!cleaned.isEmpty()) {
                        synonymText.set(cleaned);
                        context.write(synonymText, NullWritable.get());
                    }
                }
            }
        }
    }

    // Reducer: deduplicates synonym output
    public static class DeduplicateReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text synonym, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(synonym, NullWritable.get());
        }
    }

    // Driver: sets up the MapReduce job
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LngSynonymJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Extract Unique Synonyms");

        job.setJarByClass(LngSynonymJob.class);
        job.setMapperClass(SynonymExtractorMapper.class);
        job.setReducerClass(DeduplicateReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean completed = job.waitForCompletion(true);
        System.exit(completed ? 0 : 1);
    }
}
