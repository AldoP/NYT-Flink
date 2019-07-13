package myflink.Metriche;

import myflink.Constants;
import myflink.entity.CommentLog;
import myflink.utils.CommentLogSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.*;

public class Query1 {

    private static final int WINDOW_SIZE = 1;       // hours
    //private static final int WINDOW_SIZE = 24;      // hours
    //private static final int WINDOW_SIZE = 24 * 7;  // hours


    public static void run(DataStream<CommentLog> commentLog) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:29092");
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");


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


                //ADD METRICS DATA [TS INGRESSO]
        DataStream<Tuple4<CommentLog, Integer, Long, String>> timestampedAndWatermarkedT = data.map(new RichMapFunction<Tuple2<CommentLog, Integer>, Tuple4<CommentLog, Integer, Long, String>>() {

            private transient Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
            }


            @Override
            public Tuple4<CommentLog, Integer, Long, String> map(Tuple2<CommentLog, Integer> myTuple) throws Exception {
                this.meter.markEvent();
                String res2 = "\n Query_1_throughput_in , " + System.currentTimeMillis() + " , " + meter.getCount() + " , " + meter.getRate();

                return new Tuple4<>(myTuple.f0, myTuple.f1, System.currentTimeMillis(), res2);
            }


        });
        timestampedAndWatermarkedT
                .map(myTuple-> myTuple.f3).returns(Types.STRING)
                .addSink(new FlinkKafkaProducer<String>("metricheq1", new SimpleStringSchema(), properties));


        // calcola quanto un articolo e' popolare con finestra sliding
        DataStream<String> classifica = timestampedAndWatermarkedT
                .map(myTuple -> new Tuple3<>(myTuple.f0, myTuple.f1, myTuple.f2))
                .returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.INT, Types.LONG))
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


                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_window_out", new DropwizardMeterWrapper(dropwizard));

                    }

                    @Override
                    public void process(Context context, Iterable<Tuple3<CommentLog, Integer, Long>> iterable, Collector<String> collector) throws Exception {

                        String res2 = "";
                        Tuple3<Integer, CommentLog, Long> max_tuple = null;
                        boolean first = true;
                        for (Tuple3<CommentLog, Integer, Long> my_tuple : iterable) {

                            this.meter.markEvent();
                            res2 += "\n Query_1_throughput_window_out , "+System.currentTimeMillis()+" , " +meter.getCount()+" , "+meter.getRate();
                            if (first) {
                                max_tuple = new Tuple3<Integer, CommentLog, Long>(my_tuple.f1, my_tuple.f0, my_tuple.f2);
                            }
                            else if(my_tuple.f2 > max_tuple.f2) {
                                max_tuple = new Tuple3<Integer, CommentLog, Long>(my_tuple.f1, my_tuple.f0, my_tuple.f2);
                            }
                        }

                        Long localTime = System.currentTimeMillis();



                        String res = "\n Latency Query1: "+context.window().getStart();

                        res += ","+ (localTime - max_tuple.f2);
                        //System.out.print(res);
                        collector.collect(res);
                        collector.collect(res2);
                    }
                });
        classifica.addSink(new FlinkKafkaProducer<String>("metricheq1", new SimpleStringSchema(), properties));
        classifica.writeAsText(Constants.QUERY1_METRIC_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


    }
}
