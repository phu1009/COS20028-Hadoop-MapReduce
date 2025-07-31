package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordDriver {

    public static void main(String[] args) throws Exception {
        // Create Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "State Codes Count");

        // Set the main driver class
        job.setJarByClass(WordDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // Set output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and exit based on completion status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
