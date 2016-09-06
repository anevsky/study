package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PinyouidComparator extends WritableComparator {
    protected PinyouidComparator() {
        super(PinyouidTimestampWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PinyouidTimestampWritable p1 = (PinyouidTimestampWritable) w1;
        PinyouidTimestampWritable p2 = (PinyouidTimestampWritable) w2;
        return p1.getPinyouid().compareTo(p2.getPinyouid());
    }
}
