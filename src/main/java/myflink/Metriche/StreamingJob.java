package myflink.Metriche;

import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        // Create the execution environment.
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get the input data
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:29092");
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");
        DataStream<CommentLog> stream = env.addSource(
                new FlinkKafkaConsumer<>("flink", new CommentLogSchema(), properties));

        Query1.run(stream);
        //Query2.run(stream);
        //Query3.run(stream);

        env.execute();
    }
}
