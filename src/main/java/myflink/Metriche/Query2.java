package myflink.Metriche;

import com.codahale.metrics.SlidingWindowReservoir;
import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static java.lang.Float.max;

public class Query2 {

    private final static int WINDOW_SIZE = 1;
    // private final static int WINDOW_SIZE = 7;
    // private final static int WINDOW_SIZE = 30;
    private final static String PATHOUT_METRIC = "_query2_metric.out";

    public static void run(DataStream<CommentLog> stream) throws Exception{

        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "broker:29092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");

        DataStream<Tuple3<CommentLog, Long, String>> timestampedAndWatermarkedT = stream
                .filter(CommentLog::isDirect)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentLog>() {
                    @Override
                    public long extractAscendingTimestamp(CommentLog cl) {
                        return cl.getCreateDate();
                    }
                })
                .map(new RichMapFunction<CommentLog, Tuple3<CommentLog, Long, String>>() {

                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query2").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
                    }



                    @Override
                    public Tuple3<CommentLog, Long, String> map(CommentLog myCommentLog) throws Exception {
                        this.meter.markEvent();
                        String res = "\n Query_2_throughput_in , " + System.currentTimeMillis() + " , " + meter.getCount() + " , " + meter.getRate();

                        return new Tuple3<>(myCommentLog, System.currentTimeMillis(), res);
                    }


                });
                //.map(myCommentLog -> new Tuple2<>(myCommentLog, System.currentTimeMillis()))
                //.returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.LONG));
        timestampedAndWatermarkedT
                .map(myTuple-> myTuple.f2).returns(Types.STRING)
                .addSink(new FlinkKafkaProducer<String>("metricheq2", new SimpleStringSchema(), properties));


        DataStream<Tuple4<Integer, Long, Long, String>> hourlySumT = timestampedAndWatermarkedT
                .map(myTuple -> new Tuple2<>(myTuple.f0, myTuple.f1))
                .returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.LONG))

                .keyBy(myTuple -> myTuple.f0.getArticleID())
                .timeWindow(Time.minutes(120))
                .aggregate(new SumAggregator())
                .timeWindowAll(Time.minutes(120))
                .process(new InfoProcessAllWindowFunction());

        hourlySumT.map(myTuple-> myTuple.f3).returns(Types.STRING)
                .addSink(new FlinkKafkaProducer<String>("metricheq2", new SimpleStringSchema(), properties));

        DataStream<Tuple3<Integer, Long, Long>> hourlySum = hourlySumT.map(myTuple -> new Tuple3<>(myTuple.f0, myTuple.f1, myTuple.f2))
                .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG));

        DataStream<String> totalSum = hourlySum
                .keyBy(0)
                .timeWindow(Time.days(Query2.WINDOW_SIZE))
                .reduce((v1, v2) -> new Tuple3<Integer, Long, Long>(v1.f0, v1.f1 + v2.f1, Math.max(v1.f2, v1.f2)))
                .timeWindowAll(Time.days(Query2.WINDOW_SIZE))
                .process(new TotalSumProcessAllWindowFunction());

        hourlySum.print();
        totalSum.print();

        totalSum.addSink(new FlinkKafkaProducer<String>("metricheq2", new SimpleStringSchema(), properties));

        totalSum.writeAsText(Query2.WINDOW_SIZE + Query2.PATHOUT_METRIC, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

    }

    // f0 -> count
    // f1 -> ts
    private static class SumAggregator implements AggregateFunction<Tuple2<CommentLog, Long>, Tuple2<Long,Long>, Tuple2<Long, Long>>{


        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<Long, Long>(0L, System.currentTimeMillis());
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<CommentLog, Long> value, Tuple2<Long, Long> accumulator) {
            if(value.f1 < accumulator.f1){
                accumulator.f1 = value.f1;
            }
            accumulator.f0 = accumulator.f0 + 1L;
            return accumulator;

        }

        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> accumulator1, Tuple2<Long, Long> accumulator2) {

            Long sum = accumulator1.f0 + accumulator2.f0;
            Long ts = accumulator1.f1;
            if(accumulator2.f1 > ts){ ts = accumulator2.f1; }

            return new Tuple2<Long, Long>(sum, ts);

        }

    }



    // Terzo parametro TS
    private static class InfoProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple2<Long, Long>, Tuple4<Integer, Long, Long, String>, TimeWindow>{
                    //ProcessAllWindowFunction<Long, Tuple2<Integer, Long>, TimeWindow> {

        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext().getMetricGroup().addGroup("Query2").meter("throughput_out_finestra_interna", new DropwizardMeterWrapper(dropwizard));


        }


        @Override
        public void process(Context context, Iterable<Tuple2<Long, Long>> partialCounts, Collector<Tuple4<Integer, Long, Long, String>> out){
        //public void process(Context context, Iterable<Long> partialCounts, Collector<Tuple2<Integer, Long>> out) {
            Long count = 0L;
            Long ts = 0L;
            String res="";
            for (Tuple2<Long, Long> partialCount : partialCounts){

                this.meter.markEvent();

                res += "\n throughput_out_finestra_interna , " + System.currentTimeMillis() + " , " + meter.getCount() + " , " + meter.getRate();


                count += partialCount.f0;
                if(partialCount.f1 > ts){
                    ts = partialCount.f1;
                }
            }

            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            out.collect(new Tuple4<>(startDate.getHour(), count, ts, res));
        }
    }


    private static class TotalSumProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple3<Integer, Long, Long>, String, TimeWindow> {

        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext().getMetricGroup().addGroup("Query2").meter("throughput_out_finestra_finale", new DropwizardMeterWrapper(dropwizard));


        }


        @Override
        public void process(Context context, Iterable<Tuple3<Integer, Long, Long>> iterable, Collector<String> collector) {

            Tuple3<Integer, Long, Long> max_tuple = null;
            boolean first = true;
            String res2= "";
            for (Tuple3<Integer, Long, Long> my_tuple : iterable) {
                this.meter.markEvent();
                res2 += "\n Query2_throughput_out_finestra_finale , "+System.currentTimeMillis()+" , " +meter.getCount()+" , "+meter.getRate();

                if (first) {
                    max_tuple = new Tuple3<Integer, Long, Long>(my_tuple.f0, my_tuple.f1, my_tuple.f2);
                }
                else if(my_tuple.f2 > max_tuple.f2) {
                    max_tuple = new Tuple3<Integer, Long, Long>(my_tuple.f0, my_tuple.f1, my_tuple.f2);
                }
            }

            Long localTime = System.currentTimeMillis();

            String res = "\n"+context.window().getStart();

            res += ","+ (localTime - max_tuple.f2);

            //System.out.print(res);
            collector.collect(res);
            collector.collect(res2);

        }
    }
}
