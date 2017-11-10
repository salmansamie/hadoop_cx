import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *   __author__: Salman M Rahman
 */

public class Reducer_Time extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // key      : is the tweet data
        // values   : are the keys with same values (1), just dont care
        // context  : the library that parses the data

        int sum = 0;

        for (IntWritable value : values) {
            sum = sum + value.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}