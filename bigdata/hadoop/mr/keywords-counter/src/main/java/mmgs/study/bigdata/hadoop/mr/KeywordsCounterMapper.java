package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import static mmgs.study.bigdata.hadoop.mr.KeywordsCounterConstants.HEADER_FILE;

class KeywordsCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    String header;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(HEADER_FILE));
        header = bufferedReader.readLine();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!header.equals(line)) {
            StringTokenizer parsedLine = new StringTokenizer(line, "\t");
            parsedLine.nextToken();
            String keywordsToken = parsedLine.nextToken();
            StringTokenizer keywords = new StringTokenizer(keywordsToken, ",");

            Text keyword = new Text();
            while (keywords.hasMoreTokens()) {
                keyword.set(keywords.nextToken());
                context.write(keyword, one);
            }
        }
    }
}
