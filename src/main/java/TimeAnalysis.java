import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *   __author__: Salman M Rahman
 */

public class TimeAnalysis {

    private static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf);
        job.setJarByClass(TimeAnalysis.class);
        job.setMapperClass(Mapper_Time.class);
        job.setReducerClass(Reducer_Time.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);
        job.waitForCompletion(true);

        // Set job.setNumReduceTasks(value) below to run map-reduce on the cluster
        //job.setNumReduceTasks(3);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
    }

}