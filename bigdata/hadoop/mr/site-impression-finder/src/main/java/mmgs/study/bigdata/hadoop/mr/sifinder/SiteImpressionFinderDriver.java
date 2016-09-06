package mmgs.study.bigdata.hadoop.mr.sifinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.PrintStream;
import java.util.Iterator;

import static mmgs.study.bigdata.hadoop.mr.sifinder.SiteImpressionFinderConstants.APP_SITE_IMPRESSION_GROUP;

public class SiteImpressionFinderDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            printUsage(System.err);
            System.exit(2);
        }

        Job job = Job.getInstance(conf, SiteImpressionFinderConstants.APP_NAME);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SiteImpressionFinderMapper.class);
        job.setPartitionerClass(PinyouidPartitioner.class);
        job.setSortComparatorClass(PinyouidTimestampComparator.class);
        job.setGroupingComparatorClass(PinyouidComparator.class);
        job.setOutputKeyClass(PinyouidTimestampWritable.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(SiteImpressionFinderReducer.class);

        boolean exitCode = job.waitForCompletion(true);

        String maxPinYouId = "";
        long max = -1;
        Iterator<Counter> iterator = job.getCounters().getGroup(APP_SITE_IMPRESSION_GROUP).iterator();
        while (iterator.hasNext()) {
            Counter counter = iterator.next();
            if (counter.getValue() > max) {
                max = counter.getValue();
                maxPinYouId = counter.getName();
            }
        }

        if (!"".equals(maxPinYouId))
            System.out.println("\nPinYouId \"" + maxPinYouId + "\" has maximal site impression of " + max);
        else
            System.out.println("\nNo site impressions found within dataset");


        return exitCode ? 0 : 1;
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: " + SiteImpressionFinderConstants.APP_NAME + "<in> [<in>...] <out>");
    }
}
