# Use case:

There are bunch of scenarios where we need to pull data directly from kafka. I am going to list 2 here.

1. To verify if data is actually coming in from endpoint.
        Standard solution using console consumer works for this.

2. To pull history of events received from endpoint. Console consumer does not scale here, because we end up reading from beginning of the queue, which can take a very long time depending of usage of topic, retention period of topic. This is where this consumer comes to help. For usage see below.

# Compilation:
 
 To compile and generate a JAR file, checkout the code and run below command in folder containing pom.xml
 
> mvn clean compile assembly:single

JAR file will be created in target folder.

Copy the jar timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar to Kafka server.

Run the jar
> java -jar .\target\timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar

It takes bunch of arguments which are displayed in help.
<pre><code>

/project/timebased_consumer$ java -jar target/timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar 

usage: kafka_timebased_consumer.jar
 -d,--debug             Optional, Will print all messages read from
                             Kafka.
 -e,--end_time <arg>         End time in MilliSeconds where consumer will
                             end listening
 -g,--consumer_group <arg>   Name of consumer group used by this consumer.
                             For eg: test-asset-data-search
 -i,--ip_addr <arg>          IP address zookeeper
 -o,--nos_out_window <arg>   Number of out of window messages. This
                             defines when consumer will end.
 -r,--regex_string <arg>     regular expression or string to search for in
                             the messages
 -s,--start_time <arg>       Start time in MilliSeconds from where
                             consumer will start listening
 -t,--topic <arg>            Name of topic to listen
 
 For eg:  
 
 java -jar timebased_consumer-1.0-SNAPSHOT-jar-with-dependencies.jar  \
          -r '"serviceName": "%s"' \
          -t asset \
          -g kedar-asset-data-search \
          -e 1609225093 \
          -s 1609138688 \
          -i 172.31.3.191:9092 \
          -o 100
</code></pre>
