package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class SiteImpressionFinderMapper extends Mapper<LongWritable, Text, PinyouidTimestampWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(SiteImpressionFinderConstants.MAPPER_DELIMITER);
        context.write(new PinyouidTimestampWritable(tokens[2], tokens[1]), value);
    }
}
