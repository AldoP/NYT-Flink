package myflink.Metriche;

import myflink.Constants;
import myflink.entity.CommentLog;
import myflink.query3.Level2RedisMapper;
import myflink.query3.MyRedisMapper;
import myflink.utils.CommentLogSchema;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.*;


public class Query3 {

    public static void run(DataStream<CommentLog> commentLog) throws Exception {

        final int WINDOW_SIZE = 24; //in numero di ore


        //JedisPoolHolder.init("localhost", 6379);
        JedisPoolHolder.init("redis", 6379);

        //FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
        //        .setHost("localhost").build(); //aggiungere altri set

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("redis").build(); //aggiungere altri set


        // Assegna timestamp e watermark
        DataStream<Tuple2<CommentLog, Long>> timestampedAndWatermarked = commentLog
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog logIntegerTuple2) {
                        return logIntegerTuple2.getCreateDate();
                    }
                })
                .map(new RichMapFunction<CommentLog, Tuple2<CommentLog, Long>>() {

                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query3").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
                    }



                    @Override
                    public Tuple2<CommentLog, Long> map(CommentLog myCommentLog) throws Exception {
                        this.meter.markEvent();
                        return new Tuple2<>(myCommentLog, System.currentTimeMillis());
                    }


                });
                //.map(myCommentLog -> new Tuple2<>(myCommentLog, System.currentTimeMillis()))
                //.returns(Types.TUPLE(Types.POJO(CommentLog.class), Types.LONG));

        /*
         ********* Numero di Like **********
         */
        DataStream<Tuple3<String, Double, Long>> rankLike = timestampedAndWatermarked
                .filter(log -> log.f0.getDepth() == 1) // filtro i soli commenti diretti
                .map(myLog -> new Tuple3<String, Double, Long>(
                        myLog.f0.getUserID(),
                        computeNumLike(myLog.f0.getRecommendations(), myLog.f0.getEditorsSelection()),
                        myLog.f1)
                ).returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))// creo tuple (user_id, valore di a-iesimo
                .keyBy(stringDoubleTuple2 -> stringDoubleTuple2.f0)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {


                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {

                        Long timestampMin = t1.f2;

                        if (t2.f2 > timestampMin) {
                            timestampMin = t2.f2;
                        }

                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1, timestampMin);
                    }
                })
                .map(new RichMapFunction<Tuple3<String,Double, Long>, Tuple3<String,Double, Long>>() {

                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query3").meter("throughput_out_like", new DropwizardMeterWrapper(dropwizard));
                    }

                    @Override
                    public Tuple3<String, Double, Long> map(Tuple3<String, Double, Long> myTuple) throws Exception {
                        this.meter.markEvent();
                        return myTuple;

                    }



                });


        /*
         ********** Numero di commenti di risposta **********
         */

        // ******** Salvo i dati in Redis (commenti livello 1 e 2) ********

        // livello 1
        timestampedAndWatermarked
                .filter(log -> log.f0.getDepth() == 1)
                .map(myLog -> new Tuple2<String, String>(
                        myLog.f0.getCommentID(),
                        myLog.f0.getUserID()
                )).returns(Types.TUPLE(Types.STRING, Types.STRING))
                .addSink(new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper()));

        // Livello 2
        timestampedAndWatermarked
                .filter(log -> log.f0.getDepth() == 2)
                .map(myTuple -> myTuple.f0).returns(Types.POJO(CommentLog.class))
                .map(myLog -> new Tuple3<String, String, String>(
                        myLog.getCommentID(),
                        myLog.getUserID(),
                        myLog.getInReplyTo()
                )).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .map(new Level2RedisMapper())
                .filter(myTuple -> myTuple.f1!= null && myTuple.f0 != null) //test FILTRO
                .addSink(new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper()));




        //********Join con Flusso depth != 1 ******

        DataStream<Tuple3<String, Double, Long>> rankComment = timestampedAndWatermarked
                .filter(log -> log.f0.getDepth() != 1)
                .flatMap(new Level3RedisMapper())
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {
                        Long timestampMin = t1.f2;

                        if (t2.f2 > timestampMin) {
                            timestampMin = t2.f2;
                        }

                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1, timestampMin);
                    }
                });



        // ********* Risultato finale (implementa formula) ************

        DataStream<String> classificaFinale = rankComment
                .union(rankLike)
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {
                        Long timestampMin = t1.f2;

                        if (t2.f2 > timestampMin) {
                            timestampMin = t2.f2;
                        }

                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1, timestampMin);
                    }
                })
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ProcessAllWindowFunction<Tuple3<String, Double, Long>, String, TimeWindow>() {

                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query3").meter("throughput_window_out_join", new DropwizardMeterWrapper(dropwizard));
                    }


                    @Override
                    public void process(Context context, Iterable<Tuple3<String, Double, Long>> iterable, Collector<String> collector) throws Exception {
                        Tuple3<String, Double, Long> max_tuple = null;
                        boolean first = true;

                        for (Tuple3<String, Double, Long> my_tuple : iterable) {
                            this.meter.markEvent();
                            if (first) {
                                max_tuple = new Tuple3<String, Double, Long>(my_tuple.f0, my_tuple.f1, my_tuple.f2);
                            }
                            else if(my_tuple.f2 > max_tuple.f2) {
                                max_tuple = new Tuple3<String, Double, Long>(my_tuple.f0, my_tuple.f1, my_tuple.f2);
                            }
                        }

                        Long localTime = System.currentTimeMillis();
                        if(max_tuple != null){
                            String res = "\n"+context.window().getStart();

                            res += ","+ (localTime - max_tuple.f2);
                            //System.out.print(res);
                            collector.collect(res);
                        }

                    }
                });



        classificaFinale
                .writeAsText(Constants.QUERY3_METRIC_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);



    }

    private static Double computeNumLike(Integer num, Boolean isSelected){
        double w = 1;
        if(isSelected) w = 1.1;
        return  num * w * 0.3;

    }


}
