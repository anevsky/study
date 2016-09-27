package mmgs.study.bigdata.kafka;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class DirectoryBasedProducer {

    String topic;
    private Producer<String, String> producer;

    // Number of messages for which acks were received
    private long numAcked = 0;

    // Number of send attempts
    private long numSent = 0;

    // Throttle message throughput if this is set >= 0
    private long throughput;

    // Hook to trigger producing thread to stop sending messages
    private boolean stopProducing = false;

    // Directory for data source
    private String sourceDir;

    private Boolean verbose;

    public DirectoryBasedProducer(
            Properties producerProps, String topic, int throughput, String sourceDir, Boolean verbose) {

        this.topic = topic;
        this.throughput = throughput;
        this.producer = new KafkaProducer<>(producerProps);
        this.sourceDir = sourceDir;
        this.verbose = verbose;
    }

    /**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("directory-based-producer")
                .defaultHelp(true)
                .description("This tool reads file from source directory and pushes the files contents to the topic.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce messages to this topic.");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--throughput")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.");

        parser.addArgument("--acks")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .choices(0, 1, -1)
                .metavar("ACKS")
                .help("Acks required on each produced message. See Kafka docs on request.required.acks for details.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Producer config properties file.");

        parser.addArgument("--source-dir")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("SOURCE-DIR")
                .dest("sourceDir")
                .help("Where to read data from");

        parser.addArgument("--verbose")
                .action(store())
                .required(false)
                .type(Boolean.class)
                .choices(true, false)
                .metavar("VERBOSE")
                .dest("verbose")
                .help("Whether to spool messages to console (true/false)");

        return parser;
    }

    public static Properties loadProps(String filename) throws IOException {
        return org.apache.kafka.common.utils.Utils.loadProps(filename);
    }

    public static DirectoryBasedProducer createFromArgs(String[] args) {
        ArgumentParser parser = argParser();
        DirectoryBasedProducer producer = null;

        try {
            Namespace res;
            res = parser.parseArgs(args);

            String topic = res.getString("topic");
            int throughput = res.getInt("throughput");
            String configFile = res.getString("producer.config");
            String sourceDir = res.getString("sourceDir");
            Boolean verbose = res.getBoolean("verbose");

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.ACKS_CONFIG, Integer.toString(res.getInt("acks")));
            // No producer retries
            producerProps.put("retries", "0");
            if (configFile != null) {
                try {
                    producerProps.putAll(loadProps(configFile));
                } catch (IOException e) {
                    throw new ArgumentParserException(e.getMessage(), parser);
                }
            }

            if (verbose == null) {
                verbose = false;
            }
            producer = new DirectoryBasedProducer(producerProps, topic, throughput, sourceDir, verbose);

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

        return producer;
    }

    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        numSent++;
        try {
            producer.send(record, new PrintInfoCallback(key, value));
        } catch (Exception e) {

            synchronized (System.out) {
                System.out.println(errorString(e, key, value, System.currentTimeMillis()));
            }
        }
    }

    String errorString(Exception e, String key, String value, Long nowMs) {
        assert e != null : "Expected non-null exception.";

        Map<String, Object> errorData = new HashMap<>();
        errorData.put("name", "producer_send_error");

        errorData.put("time_ms", nowMs);
        errorData.put("exception", e.getClass().toString());
        errorData.put("message", e.getMessage());
        errorData.put("topic", this.topic);
        errorData.put("key", key);
        errorData.put("value", value);

        return errorData.toString();
    }

    String successString(RecordMetadata recordMetadata, String key, String value, Long nowMs) {
        assert recordMetadata != null : "Expected non-null recordMetadata object.";

        Map<String, Object> successData = new HashMap<>();
        successData.put("name", "producer_send_success");

        successData.put("time_ms", nowMs);
        successData.put("topic", this.topic);
        successData.put("partition", recordMetadata.partition());
        successData.put("offset", recordMetadata.offset());
        successData.put("key", key);
        successData.put("value", value);

        return successData.toString();
    }

    private class PrintInfoCallback implements Callback {

        private String key;
        private String value;

        PrintInfoCallback(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            synchronized (System.out) {
                if (e == null) {
                    DirectoryBasedProducer.this.numAcked++;
                    if (verbose) {
                        System.out.println(successString(recordMetadata, this.key, this.value, System.currentTimeMillis()));
                    }
                } else {
                    System.out.println(errorString(e, this.key, this.value, System.currentTimeMillis()));
                }
            }

        }
    }

    /**
     * Close the producer to flush any remaining messages.
     */
    public void close() {
        producer.close();
        System.out.println(shutdownString());
    }

    String shutdownString() {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "shutdown_complete");
        return data.toString();
    }

    public static void main(String[] args) throws IOException {
        final DirectoryBasedProducer producer = createFromArgs(args);
        final long startMs = System.currentTimeMillis();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Trigger main thread to stop producing messages
                producer.stopProducing = true;

                // Flush any remaining messages
                producer.close();

                // Print a summary
                long stopMs = System.currentTimeMillis();
                double avgThroughput = 1000 * ((producer.numAcked) / (double) (stopMs - startMs));

                Map<String, Object> data = new HashMap<>();
                data.put("name", "tool_data");
                data.put("sent", producer.numSent);
                data.put("acked", producer.numAcked);
                data.put("target_throughput", producer.throughput);
                data.put("avg_throughput", avgThroughput);

                System.out.println(data.toString());
            }
        });


        DirectoryReader reader = new DirectoryReader(producer.sourceDir);
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (producer.stopProducing) {
                break;
            }
            producer.send(null, line);
        }
    }
}
