package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.util.ToolRunner;

public class SiteImpressionFinder {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SiteImpressionFinderDriver(), args);
        System.exit(exitCode);
    }
}
