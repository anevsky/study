package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.util.ToolRunner;

public class KeywordsCounter {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new KeywordsCounterDriver(), args);
        System.exit(exitCode);
    }

}
