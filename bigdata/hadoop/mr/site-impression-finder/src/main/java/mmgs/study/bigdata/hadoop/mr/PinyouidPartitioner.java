package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PinyouidPartitioner extends Partitioner<PinyouidTimestampWritable, Text> {
    @Override
    public int getPartition(PinyouidTimestampWritable key, Text value, int i) {
        return (key.getPinyouid().hashCode() & Integer.MAX_VALUE) % i;
    }
}
