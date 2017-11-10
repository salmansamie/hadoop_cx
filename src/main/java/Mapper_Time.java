import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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

        // regexString to store the inner regex string
        String regexString = regexe + "(.*?)" + regexe;

        // now compile and match the regexString pattern
        Pattern pattern = Pattern.compile(regexString);
        Matcher matcher = pattern.matcher(tweets);

        // get all the messages in between the match at group(2)
        StringBuilder str_builder = new StringBuilder();

        // data cleaning for unixtimestamp and tweet id. Then add the bulk string to str_builder
        while (matcher.find()) {
            String tweet = matcher.group(2) + " \n";
            str_builder.append(tweet);
        }
        String sum_tweet = str_builder.toString();

        // setup for second reg match for data without device info
        String reg_map = "(;\\w+.+)";
        String reg_str = "(.*?)" + reg_map;

        // now compile and match the reg_str pattern
        Pattern p_tweet = Pattern.compile(reg_str);
        Matcher p_match = p_tweet.matcher(sum_tweet);

        // hashMap to be used for storing (key=value) for (range=counter)
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

        while (p_match.find()) {
            String index_tweet = p_match.group(1);     // index_tweet is formatted messages in bulk

            // range setup, multiples of 5, (int) casting since Math.ceil returns double
            int high = (int) (Math.ceil((float)index_tweet.length()/5)*5);
            int loww = (int) ((Math.ceil((float)index_tweet.length()/5)-1)*5 + 1);

            // print for checking range, length per index_tweet
            System.out.println(index_tweet);
            System.out.println(String.valueOf(loww) +"-"+ String.valueOf(high) +
                    "\t" + index_tweet.length());

            // string templating for ranges to be used as keys in hashMap
            String str_occurrence = (String.valueOf(loww) +"-"+ String.valueOf(high));

            // add key and set counter to 1 if the key does not exist in hashMap
            // otherwise increment the value of existing key
            if (! hashMap.containsKey(str_occurrence)){
                hashMap.put(str_occurrence, 1);
            }
            else {
                hashMap.put(str_occurrence, hashMap.get(str_occurrence)+1);
            }

            // set data for writing (Must be inside a unit iterator)
            data.set(String.valueOf(loww) + "-" + String.valueOf(high) + "\t");

            // writes to file at out (Must be inside a unit iterator)
            context.write(data, one);
        }

        System.out.println("\n\n\n" + hashMap + "\n\n\n");
    }
}
