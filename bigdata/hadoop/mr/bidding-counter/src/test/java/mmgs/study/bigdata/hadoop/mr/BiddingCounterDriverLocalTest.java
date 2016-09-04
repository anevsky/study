package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

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

    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        assertThat(outputFiles.length, is(2));

        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/driver-expected-output.bin"));
        String expectedLine;
        while ((expectedLine = expected.readLine()) != null) {
            assertThat(actual.readLine(), is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());
        actual.close();
        expected.close();
    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

}