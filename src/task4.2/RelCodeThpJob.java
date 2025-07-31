package stubs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelCodeThpJob {

    public static class CodeThpMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text resultKey = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {

            String[] columns = line.toString().split("\t");

            if (columns.length > 4) {
                String code = columns[0];
                String[] thpList = columns[4].split("/");

                for (String thpEntry : thpList) {
                    thpEntry = thpEntry.trim();
                    if (!thpEntry.isEmpty()) {
                        resultKey.set(code + "\t" + thpEntry);
                        context.write(resultKey, NullWritable.get());
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RelCodeThpJob <input_dir> <output_dir>");
            System.exit(1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Language Code - THP Relation Extractor");

        job.setJarByClass(RelCodeThpJob.class);
        job.setMapperClass(CodeThpMapper.class);
        job.setNumReduceTasks(0); // mapper-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
