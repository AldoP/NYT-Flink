package myflink;
import myflink.utils.CommentLogSchema;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;


/*
    TODO: controllare se regge il carico dello spara patate
          salvare in output
 */

public class Query1 {
    public static void main(String[] args) throws Exception {

        // Create the execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));




        // Get the input data
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<CommentLog> commentLog = env
                .addSource(new FlinkKafkaConsumer<>("test", new CommentLogSchema(), properties));
        // Parse the data, and group, windowing and aggregate it by word.
        DataStream<Tuple2<CommentLog, Integer>> data = commentLog
                .map(cl -> new Tuple2<>(cl, 1)).returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.INT));

        DataStream<Tuple2<CommentLog, Integer>> timestampedAndWatermarked = data
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<CommentLog, Integer>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple2<CommentLog, Integer> logIntegerTuple2) {
                        return logIntegerTuple2.f0.getCreateDate();
                    }
                });

        // calcola quanto un articolo e' popolare con finestra sliding
        DataStream<Tuple3<String, CommentLog, Integer>> classifica = timestampedAndWatermarked
                .keyBy(value -> value.f0.articleID)
                .timeWindow(Time.minutes(120))
                .sum(1)
                .timeWindowAll(Time.minutes(120))
                .process(new ProcessAllWindowFunction<Tuple2<CommentLog, Integer>, Tuple3<String, CommentLog, Integer>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<CommentLog, Integer>> iterable, Collector<Tuple3<String, CommentLog, Integer>> collector) throws Exception {
                        List<Tuple2<Integer, CommentLog>> tuple2s = new ArrayList<Tuple2<Integer, CommentLog>>();

                        for (Tuple2<CommentLog, Integer> my_tuple : iterable) {
                            tuple2s.add(new Tuple2<Integer, CommentLog>(my_tuple.f1, my_tuple.f0));
                        }
                        Collections.sort(tuple2s, new Comparator<Tuple2<Integer, CommentLog>>() {
                            @Override
                            public int compare(Tuple2<Integer, CommentLog> o1, Tuple2<Integer, CommentLog> o2) {
                                int v1 = o1.f0.intValue();
                                int v2 = o2.f0.intValue();
                                return v2 - v1;
                            }
                        });

                        System.out.println("--------------------");
                        Date date_start = new Date(context.window().getStart());
                        Date date_end = new Date(context.window().getEnd());
                        System.out.println("size: " + tuple2s.size());
                        System.out.println("Time Window Start " + date_start.toString());
                        System.out.println("Time Window End: " + date_end.toString());
                        if (tuple2s.size() > 0) {
                            System.out.println("\n First: " + tuple2s.get(0));
                        }
                        if (tuple2s.size() > 1) {
                            System.out.println("\n Second: " + tuple2s.get(1));
                        }
                        if (tuple2s.size() > 2) {
                            System.out.println("\n Third: " + tuple2s.get(2));
                        }

                        System.out.println("-------------------");
                    }
                });

        classifica.print().setParallelism(1);

        env.execute("Socket Window WordCount");

    }


}