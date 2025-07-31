package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelCodeNameJob {

    public static class NameSplitMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable inputKey, Text line, Context context)
                throws IOException, InterruptedException {

            String[] parts = line.toString().split("\t");

            if (parts.length > 1) {
                String code = parts[0];
                String[] nameList = parts[1].split("/");

                for (String name : nameList) {
                    String cleanedName = name.trim();

                    if (!cleanedName.isEmpty()) {
                        outputKey.set(code + "\t" + cleanedName);
                        context.write(outputKey, NullWritable.get());
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Expected usage: RelCodeNameJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Split Names by Code");

        job.setJarByClass(RelCodeNameJob.class);
        job.setMapperClass(NameSplitMapper.class);
        job.setNumReduceTasks(0); // No reducer needed

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean jobCompleted = job.waitForCompletion(true);
        System.exit(jobCompleted ? 0 : 1);
    }
}
