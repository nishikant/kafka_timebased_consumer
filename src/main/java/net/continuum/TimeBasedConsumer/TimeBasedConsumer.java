package net.continuum.TimeBasedConsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.*;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

public class TimeBasedConsumer {
    private static long kPollTimeout = 1000;
    private static int kNumRecordsToProcess = 10;

    public static void main(String[] args) {
        final Options options = new Options();

        Option oTopic = new Option("t", "topic", true, "Name of topic to listen");
        oTopic.setRequired(true);
        options.addOption(oTopic);

        Option oStartTime =
            new Option(
                       "s",
                       "start_time",
                       true,
                       "Start time in MilliSeconds from where consumer will start listening");
        oStartTime.setRequired(true);
        options.addOption(oStartTime);

        Option oEndTime =
            new Option(
                       "e", "end_time", true, "End time in MilliSeconds where consumer will end listening");
        oEndTime.setRequired(true);
        options.addOption(oEndTime);

        Option oRegexString =
            new Option(
                       "r",
                       "regex_string",
                       true,
                       "regular expression or string to search for in the messages");
        oRegexString.setRequired(true);
        options.addOption(oRegexString);

        Option oIpAddr = new Option("i", "ip_addr", true, "IP address zookeeper");
        oIpAddr.setRequired(true);
        options.addOption(oIpAddr);

        Option oConsumerGroup =
            new Option(
                       "g",
                       "consumer_group",
                       true,
                       "Name of consumer group used by this consumer. For eg: test-asset-data-search");
        oConsumerGroup.setRequired(true);
        options.addOption(oConsumerGroup);

        Option oDebug =
            new Option("d", "debug", false, "Optional, Will print all messages read from Kafka.");
        options.addOption(oDebug);

        Option oNosOutOfWindowMsg =
            new Option(
                       "o",
                       "nos_out_window",
                       true,
                       "Number of out of window messages. This defines when consumer will end.");
        oNosOutOfWindowMsg.setRequired(true);
        options.addOption(oNosOutOfWindowMsg);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("kafka_timebased_consumer.jar", options);
            System.exit(1);
        }

        String topic = cmd.getOptionValue("topic");
        Long startTimestamp = Long.parseLong(cmd.getOptionValue("start_time"));
        Long endTimestamp = Long.parseLong(cmd.getOptionValue("end_time"));
        String regexString = cmd.getOptionValue("regex_string");
        String ipAddr = cmd.getOptionValue("ip_addr");
        String consumerGroup = cmd.getOptionValue("consumer_group");
        Long nosOutOfWindowMsg = Long.parseLong(cmd.getOptionValue("nos_out_window"));
        Boolean debug = cmd.hasOption("debug");
        Pattern p = Pattern.compile(regexString);

        Properties properties = new Properties();
        properties.put(
                       ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(
                       ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipAddr);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        SeekToTimeOnRebalance seekToTimeOnRebalance =
            new SeekToTimeOnRebalance(consumer, startTimestamp);

        // subscribe to the input topic and listen for assignments.
        consumer.subscribe(Arrays.asList(topic), seekToTimeOnRebalance);

        long numRecords = 0;

        // poll and process the records.
        while (nosOutOfWindowMsg >= 0) {
            ConsumerRecords<String, String> records = consumer.poll(kPollTimeout);
            for (ConsumerRecord<String, String> record : records) {
                // The offsetsForTimes API returns the earliest offset in a topic-partition with a timestamp
                // greater than or equal to the input timestamp. There could be messages following that
                // offset
                // with timestamps lesser than the input timestamp. Let's skip such messages.
                if ((record.timestamp() < startTimestamp) && (record.timestamp() > endTimestamp)) {
                    nosOutOfWindowMsg--;
                    System.out.println(
                                       "Skipping out of order record with key "
                                       + record.key()
                                       + " timestamp "
                                       + record.timestamp());
                    continue;
                }

                String strSearch = record.value();
                Matcher m = p.matcher(strSearch);
                if (m.find() || debug) {

                    System.out.println(
                                       " record key "
                                       + record.key()
                                       + " record timestamp "
                                       + record.timestamp()
                                       + " record offset "
                                       + record.offset()
                                       + " record value "
                                       + record.value());
                }
            }
        }
        consumer.close();
    }

    public static class SeekToTimeOnRebalance implements ConsumerRebalanceListener {
        private Consumer<?, ?> consumer;
        private final Long startTimestamp;

        public SeekToTimeOnRebalance(Consumer<?, ?> consumer, Long startTimestamp) {
            this.consumer = consumer;
            this.startTimestamp = startTimestamp;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition partition : partitions) {
                timestampsToSearch.put(partition, startTimestamp);
            }
            // for each assigned partition, find the earliest offset in that partition with a timestamp
            // greater than or equal to the input timestamp
            Map<TopicPartition, OffsetAndTimestamp> outOffsets =
                consumer.offsetsForTimes(timestampsToSearch);
            for (TopicPartition partition : partitions) {
                Long seekOffset = outOffsets.get(partition).offset();
                Long currentPosition = consumer.position(partition);
                // seek to the offset returned by the offsetsForTimes API
                // if it is beyond the current position
                if (seekOffset.compareTo(currentPosition) > 0) {
                    consumer.seek(partition, seekOffset);
                }
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
    }
}
