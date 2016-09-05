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
import java.net.URISyntaxException;
import java.net.URL;

import static mmgs.study.bigdata.hadoop.mr.KeywordsCounterConstants.APP_NAME;
import static mmgs.study.bigdata.hadoop.mr.KeywordsCounterConstants.HEADER_FILE;
import static mmgs.study.bigdata.hadoop.mr.KeywordsCounterConstants.OUT_DELIMITER;

public class KeywordsCounterDriver extends Configured implements Tool {
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            printUsage(System.err);
            System.exit(2);
        }

        conf.set("mapreduce.output.textoutputformat.separator", OUT_DELIMITER);

        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(getClass());

        URL headerFile = getClass().getClassLoader().getResource(HEADER_FILE);
        job.addCacheFile(headerFile.toURI());

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
