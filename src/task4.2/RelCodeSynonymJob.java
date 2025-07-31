package stubs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelCodeSynonymJob {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text output = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if (data.length > 2) {
                String lng_code = data[0];
                String[] synonyms = data[2].split(",");

                for (String synonym : synonyms) {
                    synonym = synonym.trim();
                    if (!synonym.isEmpty()) {
                        output.set(lng_code + "\t" + synonym);
                        context.write(output, NullWritable.get());
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RelCodeSynonymJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "rel_code_synonym Job");
        job.setJarByClass(RelCodeSynonymJob.class);
        job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0); // Map-only

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}