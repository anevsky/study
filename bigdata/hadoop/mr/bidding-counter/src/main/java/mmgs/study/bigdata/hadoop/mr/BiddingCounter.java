package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class BiddingCounter {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BiddingCounterDriver(), args);
        System.exit(exitCode);

    }
}
