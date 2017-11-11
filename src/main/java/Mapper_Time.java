import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
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

        // This is routine task to convert all the data toString
        String tweets = value.toString();

        // Matches every "timestamp;tweetID;" == "([0-9]{13};[0-9]{18};)"
        String regexe = "([0-9]{13};[0-9]{18};)";

        // Now compile and match the regexString pattern
        Pattern pattern = Pattern.compile(regexe);
        Matcher matcher = pattern.matcher(tweets);

        // hashMap to be used for storing (key=value) for (hours range=counter)
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        // Part B1: data cleaning for unixtimestamp and tweet id. Then add each to array_list
        while (matcher.find()) {
            String nixTime_TwID = matcher.group(0) + '\n';

            // Converts to date & time and removes tweet id and 3 trailing zeros
            String unix_time = nixTime_TwID.substring(0, nixTime_TwID.length() - 24);

            // Date gives us the hour and minute formatted as 14:39
            String date = new java.text.SimpleDateFormat("HH:mm").format
                    (new java.util.Date (Long.parseLong(unix_time)*1000));

            // Tokenize hours and minutes by delimiters
            StringTokenizer time_token = new StringTokenizer(date);
            int hr_token = Integer.parseInt(time_token.nextToken(":"));
            int mn_token = Integer.parseInt(time_token.nextToken(":"));

            System.out.println(hr_token + "|" + mn_token);

            data.set(date);
            context.write(data, one);
        }

        //System.out.println(hashMap);
    }
}
