package stubs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngIdJob {

    public static class LngIdMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String[] data = value.toString().split("\t");

            if (data.length > 8) {
                String record = data[0] + "\t" + data[5] + "\t" + data[6] + "\t" + data[8];
                context.write(new Text(record), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LngId Extractor");

        job.setJarByClass(LngIdJob.class);
        job.setMapperClass(LngIdMapper.class);
        job.setNumReduceTasks(0); // Map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
