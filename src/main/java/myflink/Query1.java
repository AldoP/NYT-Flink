package myflink;
import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

public class Query1 {

    private static final int WINDOW_SIZE = 1;       // hours
    //private static final int WINDOW_SIZE = 24;      // hours
    //private static final int WINDOW_SIZE = 24 * 7;  // hours

    public static void main(String[] args) throws Exception {
        Query1.run();
    }

    public static void run() throws Exception {

        // Create the execution environment.
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get the input data
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "broker:29092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");
        DataStream<CommentLog> stream = env.addSource(
                new FlinkKafkaConsumer<>("flink", new CommentLogSchema(), properties));
        DataStream<CommentLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog logIntegerTuple2) {
                        return logIntegerTuple2.getCreateDate();
                    }
                });

        // calcola quanto un articolo e' popolare
        DataStream<String> chart = timestampedAndWatermarked
                .keyBy(CommentLog::getArticleID)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new SumAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ChartProcessAllWindowFunction());

        chart.print();
        chart.writeAsText(String.format(Constants.BASE_PATH + "query1_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();
    }

    private static class SumAggregator implements AggregateFunction<CommentLog, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(CommentLog value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class KeyBinder
            extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Long> counts,
                            Collector<Tuple2<String, Long>> out) {
            Long count = counts.iterator().next();
            out.collect(new Tuple2<>(key, count));
        }
    }

    private static class ChartProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) {
            List<Tuple2<String, Long>> counts = new ArrayList<>();

            for (Tuple2<String, Long> t: iterable)
                counts.add(t);

            counts.sort((a, b) -> new Long(b.f1 - a.f1).intValue());

            /*
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            LocalDateTime endDate = LocalDateTime.ofEpochSecond(
                    context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(startDate.toString() + " " + endDate.toString() + ": ");
             */
            StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() / 1000));

            int size = counts.size();
            for (int i = 0; i < 3 && i < size; i++)
                result.append(", ").append(counts.get(i).f0).append(", ").append(counts.get(i).f1);

            collector.collect(result.toString());
        }
    }
}
