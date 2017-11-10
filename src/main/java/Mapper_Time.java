import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *   __author__: Salman M Rahman
 */

public class Mapper_Time extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text data = new Text("0");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // this is routine task to convert all the data toString
        String tweets = value.toString();

        // matches every "timestamp;tweetID;" == "([0-9]{13};[0-9]{18};)"
        String regexe = "([0-9]{13};[0-9]{18};)";

        // now compile and match the regexString pattern
        Pattern pattern = Pattern.compile(regexe);
        Matcher matcher = pattern.matcher(tweets);

        // store linux times in array_list
        ArrayList<String> array_list = new ArrayList<String>();

        // hashMap to be used for storing (key=value) for (hours range=counter)
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        // data cleaning for unixtimestamp and tweet id. Then add each to array_list
        while (matcher.find()) {
            String nixTime_TwID = matcher.group(0) + '\n';

            // convert epoch to date and time and remove tweet id and trailing 3 zeros
            String unix_time = nixTime_TwID.substring(0, nixTime_TwID.length() - 24);

            // date gives us the
            String date = new java.text.SimpleDateFormat("MM/dd/YYYY HH:mm:ss").format
                    (new java.util.Date (Long.parseLong(unix_time)*1000));

            array_list.add(date);
        }

        System.out.println(array_list);

        data.set(tweets);
        context.write(data, one);
    }
}
