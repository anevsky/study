package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PinyouidComparator extends WritableComparator {
    protected PinyouidComparator() {
        super(PinyouidTimestampWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
