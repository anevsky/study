package mmgs.study.bigdata.hadoop.mr.sifinder;

import mmgs.study.bigdata.hadoop.mr.sifinder.PinyouidTimestampWritable;
import mmgs.study.bigdata.hadoop.mr.sifinder.SiteImpressionFinderMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class SiteImpressionFinderMapperTest {
    @Test
    public void map() throws Exception {
        String line = "25a4f7bb8b3ac58787016b263616bd87\t20130607131228779\tVhk0PkNTLZsTQmL\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; eSobiSubscriber 1.0.0.40; BRI/2; MAAR; InfoPath.2)\t116.226.239.*\t79\t79\t3\tersbQv1RdoTy1m58uG\td85cd6d84ab4e9e04cb153d9ae1c0f12\tnull\tSports_F_Rectangle\t300\t250\t0\t0\t50\te1af08818a6cd6bbba118bb54a651961\t254\t3476\t282163096631\t0";
        new MapDriver<LongWritable, Text, PinyouidTimestampWritable, Text>()
                .withMapper(new SiteImpressionFinderMapper())
                .withInput(new LongWritable((short) 0), new Text(line))
                .withOutput(new PinyouidTimestampWritable("Vhk0PkNTLZsTQmL", "20130607131228779"), new Text(line))
                .runTest();
    }

}