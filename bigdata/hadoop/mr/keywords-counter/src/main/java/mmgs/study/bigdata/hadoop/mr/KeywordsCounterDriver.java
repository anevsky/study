package mmgs.study.bigdata.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.io.PrintStream;

import static mmgs.study.bigdata.hadoop.mr.KeywordsCounterConstants.APP_NAME;

class KeywordsCounterDriver extends Configured implements Tool {
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
/*        if (otherArgs.length == 1 && args[0] == "-help") {
            printUsage(System.out);
            System.exit(0);
        } else*/
        if (otherArgs.length < 2) {
            printUsage(System.err);
            System.exit(2);
        }

        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(KeywordsCounterMapper.class);
        job.setCombinerClass(KeywordsCounterReducer.class);
        job.setReducerClass(KeywordsCounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: " + APP_NAME + "<in> [<in>...] <out>");
    }
}
