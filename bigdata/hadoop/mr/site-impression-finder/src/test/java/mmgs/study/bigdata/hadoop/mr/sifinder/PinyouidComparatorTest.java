package mmgs.study.bigdata.hadoop.mr.sifinder;

import mmgs.study.bigdata.hadoop.mr.sifinder.PinyouidComparator;
import mmgs.study.bigdata.hadoop.mr.sifinder.PinyouidTimestampWritable;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;

public class PinyouidComparatorTest {
    private final PinyouidComparator comparator = new PinyouidComparator();

    @Test
    public void compareNotEqual() throws Exception {
        PinyouidTimestampWritable recordLower = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606000829000");
        PinyouidTimestampWritable recordHigher = new PinyouidTimestampWritable("ZYqzZA5EDlzWqt", "20130606001605800");
        int result = comparator.compare(recordLower, recordHigher);
        assertThat("Lower is less than higher", result, lessThan(0));
    }

    public void compareEqual() throws Exception {
        PinyouidTimestampWritable record1 = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606000829000");
        PinyouidTimestampWritable record2 = new PinyouidTimestampWritable("Vh1_O55PDtSUDQb", "20130606001605800");
        int result = comparator.compare(record1, record2);
        assertThat("Objects are equal", result, is(0));
    }

}