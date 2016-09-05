package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BiddingCounterDriverLocalTest {
    @Test
    public void runLocalValid() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        URL testFile = getClass().getResource("/test-bidding-dataset.txt");
        Path input = new Path(testFile.getPath());

        URL outDir = getClass().getResource("/output");
        Path output = new Path(outDir.getPath());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        BiddingCounterDriver driver = new BiddingCounterDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]{input.toString(), output.toString()});

        assertThat(exitCode, is(0));
        checkOutput(conf, output);
    }

    /**
     * Read sequence file generated by MapReduce, convert it to text and compare with reference expected results
     */
    private void checkOutput(Configuration conf, Path output) throws IOException {
        // check that the required number of files is generated
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        // 2 = actual output + SUCCESS file
        assertThat(outputFiles.length, is(2));
        System.out.println(outputFiles[0].toString() + " " + outputFiles[1].toString());

        // read expected-results file
        BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/driver-expected-output.txt"));

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(outputFiles[1]));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            String expectedLine;
            while ((reader.next(key, value)) && ((expectedLine = expected.readLine()) != null)) {
                assertThat((key + "\t" + value), is(expectedLine));
            }
        } finally {
            expected.close();
            IOUtils.closeStream(reader);
        }
    }


    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

}