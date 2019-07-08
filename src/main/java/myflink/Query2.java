package myflink;

import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class Query2 {

    private final static int WINDOW_SIZE = 24 * 7;
    private final static String PATHOUT = "_query2.out";

    public static void main(String[] args) throws Exception{

        // Create the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Get the input data
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<CommentLog> stream = env
                .addSource(new FlinkKafkaConsumer<>("test", new CommentLogSchema(), properties));
        /*
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentLog>() {
            @Override
            public long extractAscendingTimestamp(CommentLog element) {
                long timestamp = element.getCreateDate() / 1000;
                LocalDateTime curr = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
                System.out.println(timestamp);
                System.out.println(curr.toString());
                int roundedHour = curr.getHour() / 2 * 2;
                System.out.println(roundedHour);
                LocalDateTime rounded = LocalDateTime.of(
                        curr.getYear(), curr.getMonth(), curr.getDayOfMonth(), roundedHour, 0);
                System.out.println(rounded.toString());
                return rounded.toEpochSecond(ZoneOffset.UTC) * 1000;
            }
        });
        */

        DataStream<CommentLog> timestampedAndWatermarked = stream
                .filter(CommentLog::isDirect)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentLog>() {
                    @Override
                    public long extractAscendingTimestamp(CommentLog cl) {
                        return cl.getCreateDate();
                    }
                });

        DataStream<Tuple2<Integer, Long>> hourlySum = timestampedAndWatermarked
        //DataStream<Tuple3<String, String, Long>> hourlySum = timestampedAndWatermarked
        //DataStream<Tuple3<String, String, Long>> hourlySum = stream
                .keyBy(value -> value.articleID)
                .timeWindow(Time.minutes(120))
                //.aggregate(new SumAggregator(), new InfoProcessWindowFunction());
                .aggregate(new SumAggregator())
                .timeWindowAll(Time.minutes(120))
                .process(new InfoProcessAllWindowFunction());

        DataStream<String> totalSum = hourlySum
                .keyBy(0)
                .timeWindow(Time.hours(Query2.WINDOW_SIZE))
                .reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1))
                .timeWindowAll(Time.hours(Query2.WINDOW_SIZE))
                .process(new TotalSumProcessAllWindowFunction());

        hourlySum.print();
        totalSum.print();
        totalSum.writeAsText(Query2.WINDOW_SIZE + Query2.PATHOUT, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

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



    private static class InfoProcessWindowFunction
            extends ProcessWindowFunction<Long, Tuple3<String, String, Long>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Long> counts,
                            Collector<Tuple3<String, String, Long>> out) {
            Long count = counts.iterator().next();
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            LocalDateTime endDate = LocalDateTime.ofEpochSecond(
                    context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            String windowRange = startDate.toString() + " " + endDate.toString();
            out.collect(new Tuple3<>(windowRange, key, count));
        }
    }



    private static class InfoProcessAllWindowFunction
            extends ProcessAllWindowFunction<Long, Tuple2<Integer, Long>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Long> partialCounts, Collector<Tuple2<Integer, Long>> out) {
            Long count = 0L;
            for (Long partialCount : partialCounts)
                count += partialCount;
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            //LocalDateTime endDate = LocalDateTime.ofEpochSecond(
            //        context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            //String windowRange = startDate.toString() + " " + endDate.toString();
            //out.collect(new Tuple2<>(windowRange, count));
            out.collect(new Tuple2<>(startDate.getHour(), count));
        }
    }



    private static class TotalSumProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, Long>> iterable, Collector<String> collector) {
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            LocalDateTime endDate = LocalDateTime.ofEpochSecond(
                    context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(startDate.toString() + " " + endDate.toString() + ": ");
            List<Tuple2<Integer, Long>> sortedList = new ArrayList<>();
            for (Tuple2<Integer, Long> t : iterable)
                sortedList.add(t);
            sortedList.sort(Comparator.comparingInt(a -> a.f0));
            for (Tuple2<Integer, Long> hourlySum : sortedList)
                result.append(hourlySum.f0.toString()).append(":").append(hourlySum.f1.toString()).append(" ");
            collector.collect(result.toString());
        }
    }
}
