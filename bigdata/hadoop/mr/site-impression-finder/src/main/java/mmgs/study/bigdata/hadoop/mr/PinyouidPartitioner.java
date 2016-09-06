package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PinyouidPartitioner extends Partitioner<PinyouidTimestampWritable, NullWritable> {
    @Override
    public int getPartition(PinyouidTimestampWritable p, NullWritable nullWritable, int i) {
        return (p.getPinyouid().hashCode() * 137) % i;
    }
}
