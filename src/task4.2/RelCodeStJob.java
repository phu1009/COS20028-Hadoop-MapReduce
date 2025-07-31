package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelCodeStJob {

    // Mapper class to extract language code and status
    public static class CodeStatusMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t");

            if (fields.length > 7) {
                String code = fields[0];
                String status = fields[7].trim();

                if (!status.isEmpty()) {
                    outputKey.set(code + "\t" + status);
                    context.write(outputKey, NullWritable.get());
                }
            }
        }
    }

    // Driver method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RelCodeStJob <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Language Code and Status Extraction");

        job.setJarByClass(RelCodeStJob.class);
        job.setMapperClass(CodeStatusMapper.class);
        job.setNumReduceTasks(0); // Mapper-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
