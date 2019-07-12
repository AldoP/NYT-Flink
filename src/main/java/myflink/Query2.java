package myflink;

import myflink.entity.CommentLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class Query2 {

    private final static int WINDOW_SIZE = 1;
    //private final static int WINDOW_SIZE = 7;
    //private final static int WINDOW_SIZE = 30;

    public static void run(DataStream<CommentLog> stream) throws Exception{

        DataStream<CommentLog> timestampedAndWatermarked = stream
                .filter(CommentLog::isDirect)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog cl) {
                        return cl.getCreateDate();
                    }
                });

        DataStream<Tuple2<Integer, Long>> hourlySum = timestampedAndWatermarked
                .keyBy(CommentLog::getArticleID)
                .timeWindow(Time.minutes(120))
                .aggregate(new SumAggregator())
                .timeWindowAll(Time.minutes(120))
                .process(new InfoProcessAllWindowFunction());

        DataStream<String> totalSum = hourlySum
                .keyBy(0)
                .timeWindow(Time.days(WINDOW_SIZE))
                .reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1))
                .timeWindowAll(Time.days(WINDOW_SIZE))
                .process(new TotalSumProcessAllWindowFunction());

        //hourlySum.print();
        totalSum.print();
        totalSum.writeAsText(String.format(Constants.BASE_PATH + "query2_%d.out", WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
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


    private static class InfoProcessAllWindowFunction
            extends ProcessAllWindowFunction<Long, Tuple2<Integer, Long>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Long> partialCounts, Collector<Tuple2<Integer, Long>> out) {
            Long count = 0L;
            for (Long partialCount : partialCounts)
                count += partialCount;
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            out.collect(new Tuple2<>(startDate.getHour(), count));
        }
    }


    private static class TotalSumProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, Long>> iterable, Collector<String> collector) {

            /*
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            LocalDateTime endDate = LocalDateTime.ofEpochSecond(
                    context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(startDate.toString() + " " + endDate.toString() + ": ");
             */
            StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() / 1000));

            // Sort time slots
            List<Tuple2<Integer, Long>> sortedList = new ArrayList<>();
            for (Tuple2<Integer, Long> t : iterable)
                sortedList.add(t);
            sortedList.sort(Comparator.comparingInt(a -> a.f0));

            // Build result string
            int hour = 0;
            for (Tuple2<Integer, Long> hourlySum : sortedList) {
                // fill missing time slots
                while (!hourlySum.f0.equals(hour)) {
                    //result.append(hour).append(":").append("0 ");
                    result.append(", 0");
                    hour += 2;
                }
                //result.append(hourlySum.f0.toString()).append(":").append(hourlySum.f1.toString()).append(" ");
                result.append(", ").append(hourlySum.f1.toString());
                hour += 2;
            }
            // fill remaining missing time slots
            for (; hour != 24; hour += 2)
                result.append(", 0");
                //result.append(hour).append(":").append("0 ");

            collector.collect(result.toString());
        }
    }
}
