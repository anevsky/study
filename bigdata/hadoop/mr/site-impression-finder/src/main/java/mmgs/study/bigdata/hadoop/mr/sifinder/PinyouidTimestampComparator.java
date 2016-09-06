package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class PinyouidTimestampComparator extends WritableComparator {
    PinyouidTimestampComparator() {
        super(PinyouidTimestampWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PinyouidTimestampWritable p1 = (PinyouidTimestampWritable) w1;
        PinyouidTimestampWritable p2 = (PinyouidTimestampWritable) w2;
        int c1 = p1.getPinyouid().compareTo(p2.getPinyouid());
        return c1 != 0 ? c1 : p1.getTimestampTxt().compareTo(p2.getTimestampTxt());
    }
}

