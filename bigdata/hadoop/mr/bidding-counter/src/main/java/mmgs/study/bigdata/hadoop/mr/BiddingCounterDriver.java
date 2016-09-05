package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.PrintStream;

import static mmgs.study.bigdata.hadoop.mr.BiddingCounterConstants.APP_NAME;

public class BiddingCounterDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            printUsage(System.err);
            System.exit(2);
        }

        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BiddingCounterMapper.class);
        job.setCombinerClass(BiddingCounterReducer.class);
        job.setReducerClass(BiddingCounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HitPriceWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // TODO: java.lang.RuntimeException: native snappy library not available:
        //       this version of libhadoop was built without snappy support.
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: " + APP_NAME + "<in> [<in>...] <out>");
    }

}
