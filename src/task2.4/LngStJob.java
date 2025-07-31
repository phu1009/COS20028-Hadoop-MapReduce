package stubs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LngStJob {

    public static class LngStMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String[] data = value.toString().split("\t");

            if (data.length > 7) {
                String st = data[7].trim();

                if (!st.isEmpty()) {
                    context.write(new Text(st), NullWritable.get());
                }
            }
        }
    }

    public static class LngStReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LngSt Extractor");

        job.setJarByClass(LngStJob.class);
        job.setMapperClass(LngStMapper.class);
        job.setReducerClass(LngStReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
