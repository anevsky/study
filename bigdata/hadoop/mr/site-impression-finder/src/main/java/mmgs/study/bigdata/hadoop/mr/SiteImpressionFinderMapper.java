package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static mmgs.study.bigdata.hadoop.mr.SiteImpressionFinderConstants.MAPPER_DELIMITER;

class SiteImpressionFinderMapper extends Mapper<LongWritable, Text, PinyouidTimestampWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(MAPPER_DELIMITER);
        context.write(new PinyouidTimestampWritable(tokens[2], tokens[1]), new Text(tokens[21]));
    }
}
