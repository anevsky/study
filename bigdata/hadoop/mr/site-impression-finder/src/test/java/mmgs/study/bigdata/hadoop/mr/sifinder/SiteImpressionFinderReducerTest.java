package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

public class SiteImpressionFinderReducerTest extends Reducer<PinyouidTimestampWritable, Text, NullWritable, Text>{
    @Test
    public void reduce() throws Exception {

    }

}