Use case:

There are bunch of scenarios where we need to pull data directly from kafka. I am going to list 2 here.

1. To verify if data is actually coming in from endpoint.

        Standard solution using console consumer works for this.

2. To pull history of events received from endpoint, console consumer does not scale here, because we end up reading from beginning of the queue, which can take a very long time depending of usage of topic, retention period of topic. This is where this consumer comes to help. For usage see below.

Run:
 
mvn clean compile assembly:single

To generate the jar in target folder. 

Copy the jar for eg: timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar to zookeeper server.

Run the jar 
java -jar .\target\timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar

It takes bunch of arguments which are displayed in help.

/project/timebased_consumer$ java -jar target/timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar 
Missing required options: t, s, e, r, i, g

usage: kafka_timebased_consumer.jar
 -e,--end_time <arg>         End time in MilliSeconds where consumer will
                             end listening
 -g,--consumer_group <arg>   Name of consumer group used by this consumer.
                             For eg: test-asset-data-search
 -i,--ip_addr <arg>          IP address zookeeper
 -r,--regex_string <arg>     regular expression or string to search for in
                             the messages
 -s,--start_time <arg>       Start time in MilliSeconds from where
                             consumer will start listening
 -t,--topic <arg>            Name of topic to listen
/project/timebased_consumer$ 
