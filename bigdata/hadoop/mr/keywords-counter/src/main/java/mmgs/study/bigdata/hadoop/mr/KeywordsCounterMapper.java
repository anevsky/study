package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

class KeywordsCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer line = new StringTokenizer(value.toString(), "\t");
        line.nextToken();
        String keywordsToken = line.nextToken();
        StringTokenizer keywords = new StringTokenizer(keywordsToken, ",");

        Text keyword = new Text();
        while (keywords.hasMoreTokens()) {
            keyword.set(keywords.nextToken());
            context.write(keyword, one);
        }
    }
}
