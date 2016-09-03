package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class KeywordsCounterMapper extends Mapper<Text, LongWritable, Text, LongWritable>{
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {

    }
}
