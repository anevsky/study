package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;

public class SiteImpressionFinderReducerTest extends Reducer<PinyouidTimestampWritable, Text, NullWritable, Text>{
    @Test
    public void reduce() throws Exception {
        Text line = new Text("e70ad0e8940fcab493b22589d1525cdc\t20130606000226000\t6gfYZDn7O6LyDIl_egbsQJEHgekTaQE\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0),gzip(gfe),gzip(gfe)\t118.121.116.*\t276\t283\t2\tDSmreF9aBTN7gqKbuKz\taa623be7f8c644e8911acb7abd5d7112\t\t3226700719\t160\t600\t2\t0\t5\tcb7c76e7784031272e37af8e7e9b062c\t300\t1458\t282825712806\t0");
        new ReduceDriver<PinyouidTimestampWritable, Text, NullWritable, Text>()
                .withReducer(new SiteImpressionFinderReducer())
                .withInput(new PinyouidTimestampWritable("6gfYZDn7O6LyDIl_egbsQJEHgekTaQE", "20130606000226000"),
                        Arrays.asList(line))
                .withOutput(NullWritable.get(), line)
                .runTest();
    }

}