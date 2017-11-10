import java.io.IOException;
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

        // hashMap to be used for storing (key=value) for (hours range=counter)
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        // set regex and compile patterns separately as hr and mn
        String hr_reg = "(\\b\\d{2}:)";

        Pattern hr = Pattern.compile(hr_reg);


        // data cleaning for unixtimestamp and tweet id. Then add each to array_list
        while (matcher.find()) {
            String nixTime_TwID = matcher.group(0) + '\n';

            // convert epoch to date and time and remove tweet id and trailing 3 zeros
            String unix_time = nixTime_TwID.substring(0, nixTime_TwID.length() - 24);

            // date gives us the hour and minute as 14:39
            String date = new java.text.SimpleDateFormat("HH:mm").format
                    (new java.util.Date (Long.parseLong(unix_time)*1000));

            // matchers for hr and mn
            Matcher mat_hr = hr.matcher(date);

            String x = "";
            while(mat_hr.find()){
                x = mat_hr.group(0);
                x = x.substring(0, x.length() - 1);
            }
            int xx = Integer.parseInt(x);
            String high_range = String.valueOf(xx);
            String loww_range = String.valueOf((Integer.parseInt(x) + 1));
            String z = (String.valueOf(high_range) +"-"+ String.valueOf(loww_range));

            if (!hashMap.containsKey(z)){
                hashMap.put(z, 1);
            }
            else{
                hashMap.put(z, hashMap.get(z) + 1);
            }
            data.set(z);
            context.write(data, one);
        }

        System.out.println(hashMap);
    }
}
