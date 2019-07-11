package myflink.Metriche;

import myflink.Constants;
import myflink.MyMapMetrics;
import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class Query1 {

    private static final int WINDOW_SIZE = 1;       // hours
    //private static final int WINDOW_SIZE = 24;      // hours
    //private static final int WINDOW_SIZE = 24 * 7;  // hours

    public static void main(String[] args) throws Exception {

        // Create the execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get the input data
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<CommentLog> commentLog = env
                .addSource(new FlinkKafkaConsumer<>("test", new CommentLogSchema(), properties));
        // Parse the data, and group, windowing and aggregate it by word.
        DataStream<Tuple2<CommentLog, Integer>> data = commentLog
                .map(cl -> new Tuple2<>(cl, 1)).returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.INT));

        DataStream<Tuple3<CommentLog, Integer, Long>> timestampedAndWatermarked = data
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<CommentLog, Integer>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple2<CommentLog, Integer> logIntegerTuple2) {
                        return logIntegerTuple2.f0.getCreateDate();
                    }
                })
                .map(myTuple2 -> new Tuple3<>(myTuple2.f0, myTuple2.f1, System.currentTimeMillis()))
                .returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.INT, Types.LONG));

        // calcola quanto un articolo e' popolare con finestra sliding
        DataStream<String> classifica = timestampedAndWatermarked
                .keyBy(value -> value.f0.articleID)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .reduce(new ReduceFunction<Tuple3<CommentLog, Integer, Long>>() {
                    @Override
                    public Tuple3<CommentLog, Integer, Long> reduce(Tuple3<CommentLog, Integer, Long> t1, Tuple3<CommentLog, Integer, Long> t2) throws Exception {
                        Long timestampMin = t1.f2;

                        if(t2.f2 > timestampMin){ timestampMin = t2.f2;}

                        return new Tuple3<>(t1.f0, t1.f1+t2.f1, timestampMin);

                    }
                })
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ProcessAllWindowFunction<Tuple3<CommentLog, Integer, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple3<CommentLog, Integer, Long>> iterable, Collector<String> collector) throws Exception {

                        Tuple3<Integer, CommentLog, Long> max_tuple = null;
                        boolean first = true;
                        for (Tuple3<CommentLog, Integer, Long> my_tuple : iterable) {
                            if (first) {
                                max_tuple = new Tuple3<Integer, CommentLog, Long>(my_tuple.f1, my_tuple.f0, my_tuple.f2);
                            }
                            else if(my_tuple.f2 > max_tuple.f2) {
                                max_tuple = new Tuple3<Integer, CommentLog, Long>(my_tuple.f1, my_tuple.f0, my_tuple.f2);
                            }
                        }

                        Long localTime = System.currentTimeMillis();



                        String res = "\n"+context.window().getStart();

                        res += ","+ (localTime - max_tuple.f2);
                        //System.out.print(res);
                        collector.collect(res);
                    }
                });

        classifica.writeAsText(Constants.QUERY1_METRIC_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Socket Window WordCount");

    }

}
