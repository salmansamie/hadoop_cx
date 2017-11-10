import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.annotation.PostConstruct;

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
        ArrayList<Long> array_list = new ArrayList<Long>();

        // data cleaning for unixtimestamp and tweet id. Then add the bulk string to str_builder
        while (matcher.find()) {
            String unix_time = matcher.group(0) + '\n';
            unix_time = unix_time.substring(0, unix_time.length() - 21);

            Long time_int = Long.parseLong(unix_time);
            array_list.add(time_int);
        }

        System.out.println(array_list);

        data.set(tweets);
        context.write(data, one);

    }
}
