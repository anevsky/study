package mmgs.study.bigdata.kafka;

import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DirectoryReaderTest {
    @Test
    public void readDataFromExistingDirectory() throws Exception {
        String raw_data = getClass().getClassLoader().getResource("raw_data").getPath();

        DirectoryReader reader = new DirectoryReader(raw_data);

        List<String> readLines = new ArrayList<>(3);
        String line = null;
        while ((line = reader.readLine()) != null) {
            readLines.add(line);
        }

        List<String> expectedLines = Arrays.asList("39b90003681aac2c69fdebc85c9e117a\t20130606081914461\tZ0T3CpT-PvF-kBb\tmozilla/4.0 (compatible; msie 7.0; windows nt 5.1)\t110.18.20.*\t27\t31\t1\ttrqRTvFRLpscFU\tfa08a89e3235004ad2c79e3b21cebb31\t\tmm_10027070_2459574_9659312\t160\t600\t2\t1\t0\t62f7f9a6dca2f80cc00f17dcda730bc1\t227\t3427\t282162999974\t1",
                "b04b239447f9a3d68bd7f51cb8eea6d7\t20130607103311054\tVhkRZ7slDqlfkOz\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4\t118.244.181.*\t55\t57\t3\t3SCYZrn0Qo18XMB4JKTI\t55296ab4b4cf5effa7bfad1764700d86\tnull\tdiscuz_18316225_005\t728\t90\t0\t0\t20\tb90c12ed2bd7950c6027bf9c6937c48a\t300\t3386\t282825713042\t0",
                "e3001476e0c1fbceabe90d926fce43da\t20130608121721208\tnull\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; QQDownload 718)\t58.221.56.*\t80\t86\t3\t5D1El5C0gQ27gspy\t3c177b7a1b758d2018856f2037f1180\tnull\tLV_1001_LDVi_LD_ADX_4\t300\t250\t0\t0\t100\ta499988a822facd86dd0e8e4ffef8532\t300\t1458\t282825712806\t0");

        assertThat("Three readLines read", 3, is(readLines.size()));
        assertThat("Expected file exists", expectedLines, is(readLines));
    }

}