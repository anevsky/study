package mmgs.study.bigdata.spark.kwmatcher;

import mmgs.study.bigdata.spark.kwmatcher.crawler.MeetupCrawler;
import mmgs.study.bigdata.spark.kwmatcher.crawler.SNCrawler;
import mmgs.study.bigdata.spark.kwmatcher.crawler.SNItem;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedSN;
import mmgs.study.bigdata.spark.kwmatcher.model.WeightedKeyword;
import mmgs.study.bigdata.spark.kwmatcher.storage.HBaseStorage;
import mmgs.study.bigdata.spark.kwmatcher.storage.Storage;
import mmgs.study.bigdata.spark.kwmatcher.tokenizer.KeywordsExtractor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeywordsMatcher {

    public static void main(String[] args) {
        // Initialize spark application
        // TODO: parameters must be configured via separate parameter file
        SparkConf sparkConf = new SparkConf()
                .setAppName("Keywords Matcher")
                // TODO: move master to parameters
                .setMaster("local[*]")
                // TODO: move spark.executor.memory to parameters
                .set("spark.executor.memory", "2g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{TaggedClick.class, TaggedSN.class});

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // TODO: move sql context to reader/writer
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);

        // initialize meetup connection keys
        // Stub for keys
        // TODO: provide file path as a parameter
        JavaRDD<String> keysRDD = javaSparkContext.textFile("meetupkeys");
        List<String> keysArr = keysRDD.collect();
        javaSparkContext.broadcast(keysArr);

        // initialize click dataset
        Storage storage = new HBaseStorage();
        // for each row we need to take one pseudo-random meetup key
        JavaRDD<TaggedClick> taggedClicksRDD = storage.readTaggedClicks(sqlContext);

        SNCrawler crawler = new MeetupCrawler();

        // transformations
        // extract keywords for each click
        // TODO: make sure that keywords are delimited by spaces
        JavaPairRDD<TaggedClick, List<WeightedKeyword>> enrichedTaggedClicksRDD = taggedClicksRDD.flatMapToPair(new SNKeywordsMapper(crawler, keysArr));

        // combine clicks for identical
        JavaPairRDD<TaggedClick, List<WeightedKeyword>> aggregatedTaggedClicksRDD = enrichedTaggedClicksRDD.reduceByKey(new SNKeywordsReducer());

        // convert to final dataset structure
        JavaRDD<TaggedSN> snTaggedRDD = aggregatedTaggedClicksRDD.map(new SNTaggedClickMapper());

        HiveContext hiveContext = new HiveContext(javaSparkContext);
        DataFrame dataFrame = hiveContext.createDataFrame(snTaggedRDD.rdd(), TaggedSN.class);

        // debugging
        dataFrame.show();

        // save as hive table
        // TODO: generate valid file name
        dataFrame.write().format("orc").option("header", "false").save("tstkw");
    }

    private static class SNKeywordsReducer implements Function2<List<WeightedKeyword>, List<WeightedKeyword>, List<WeightedKeyword>> {
        @Override
        public List<WeightedKeyword> call(List<WeightedKeyword> keywords1, List<WeightedKeyword> keywords2) throws Exception {
            List<WeightedKeyword> keywords = new ArrayList<>();
            keywords.addAll(keywords1);
            keywords.addAll(keywords2);
            keywords.sort(Comparator.comparing(WeightedKeyword::getFrequency).reversed());
            return new ArrayList<>(keywords.subList(0, Math.min(10, keywords1.size() + keywords2.size())));
        }
    }

    private static class SNKeywordsMapper implements PairFlatMapFunction<TaggedClick, TaggedClick, List<WeightedKeyword>> {
        private final SNCrawler crawler;
        private final Random random;
        private final List<String> keys;

        public SNKeywordsMapper(SNCrawler crawler, List<String> keys) {
            this.crawler = crawler;
            this.random = new Random();
            this.keys = Collections.unmodifiableList(keys);
        }

        @Override
        public Iterable<Tuple2<TaggedClick, List<WeightedKeyword>>> call(TaggedClick taggedClick) throws Exception {
            int i = random.nextInt(keys.size());
            String key = keys.get(i);
            List<SNItem> snEvents = crawler.extractEvents(taggedClick, key);
            Stream<List<WeightedKeyword>> listStream = snEvents.stream().map(x -> {
                try {
                    return KeywordsExtractor.getTopN(x.getDescription(), 10);
                } catch (IOException e) {
                    // TODO: handle exception properly
                    e.printStackTrace();
                    return new ArrayList<WeightedKeyword>();
                }
            });
            taggedClick.clearId();
            return listStream.map(x -> new Tuple2<>(taggedClick, x))::iterator;
        }
    }

    private static class SNTaggedClickMapper implements Function<Tuple2<TaggedClick, List<WeightedKeyword>>, TaggedSN> {
        @Override
        public TaggedSN call(Tuple2<TaggedClick, List<WeightedKeyword>> taggedClickListTuple2) throws Exception {
            return new TaggedSN(taggedClickListTuple2._1(), taggedClickListTuple2._2().stream().map(x -> x.getKeyword()).collect(Collectors.joining(" ")));
        }
    }
}

