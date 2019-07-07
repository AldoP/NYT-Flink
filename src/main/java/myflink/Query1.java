package myflink;
import myflink.utils.CommentLogSchema;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
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
        RocksDBStateBackend my_rocksDB = new RocksDBStateBackend("file:///tmp");
        env.setStateBackend(my_rocksDB);

        final int WINDOW_SIZE = 24*7; //in numero di ore

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
        DataStream<String> classifica = timestampedAndWatermarked
                .keyBy(value -> value.f0.articleID)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .sum(1)
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ProcessAllWindowFunction<Tuple2<CommentLog, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<CommentLog, Integer>> iterable, Collector<String> collector) throws Exception {
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
                        String res = " ";
                        res += "--------------------";
                        Date date_start = new Date(context.window().getStart());
                        // Date date_end = new Date(context.window().getEnd());
                        System.out.println("Time Window Start " + date_start.toString());
                        res += "\nts: "+context.window().getStart();
                        int size = tuple2s.size();
                        for (int i = 0; i < 3 && i < size; i++) {
                            res += "\n artID_" + (i + 1) + "  : " + tuple2s.get(i).f1.getArticleID();
                            res += "\n nCmnt_" + (i + 1) + "  : " + tuple2s.get(i).f0;
                        }

                        res += "\n-------------------";
                        System.out.println(res);
                        collector.collect(res);

                    }
                });

        classifica.writeAsText(Constants.QUERY1_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Socket Window WordCount");

    }


}