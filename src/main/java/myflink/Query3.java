package myflink;

import myflink.entity.CommentLog;
import myflink.query3.Level2RedisMapper;
import myflink.query3.Level3RedisMapper;
import myflink.query3.MyRedisMapper;
import myflink.utils.CommentLogSchema;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.*;

/*
        TODO: sparapatate troppo lento

 */
public class Query3 {

    public static void run(DataStream<CommentLog> stream) throws Exception {

        final int WINDOW_SIZE = 24*30; //in numero di ore
        boolean docker = false;


        if(docker) {JedisPoolHolder.init("redis", 6379);}
        else{ JedisPoolHolder.init("localhost", 6379); }

        JedisPoolHolder.init("localhost", 6379);


        FlinkJedisPoolConfig conf;

        if(docker){ conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build(); }
        else {      conf = new FlinkJedisPoolConfig.Builder() .setHost("localhost").setPort(6379).build();}



        // Assegna timestamp e watermark
        DataStream<CommentLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog logIntegerTuple2) {
                        return logIntegerTuple2.getCreateDate();
                    }
                });

        /*
         ********* Numero di Like **********
         */
        DataStream<Tuple2<String, Double>> rankLike = timestampedAndWatermarked
                .filter(log -> log.getDepth() == 1) // filtro i soli commenti diretti
                .map(myLog -> new Tuple2<String, Double>(
                        myLog.getUserID(),
                        computeNumLike (myLog.getRecommendations(), myLog.getEditorsSelection()))
                ).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))// creo tuple (user_id, valore di a-iesimo
                .keyBy(stringDoubleTuple2 -> stringDoubleTuple2.f0)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .sum(1);

        /*

         ********** Numero di commenti di risposta **********
         */

        // ******** Salvo i dati in Redis (commenti livello 1 e 2) ********

        // livello 1
        timestampedAndWatermarked
                .filter(log -> log.getDepth() == 1)
                .map(myLog -> new Tuple2<String, String>(
                        myLog.getCommentID(),
                        myLog.getUserID()
                )).returns(Types.TUPLE(Types.STRING, Types.STRING))
                .addSink(new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper()));

        // Livello 2
        timestampedAndWatermarked
                .filter(log -> log.getDepth() == 2)
                .map(myLog -> new Tuple3<String, String, String>(
                        myLog.getCommentID(),
                        myLog.getUserID(),
                        myLog.getInReplyTo()
                )).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .map(new Level2RedisMapper())
                .filter(myTuple -> myTuple.f1!= null && myTuple.f0 != null) //test FILTRO
                .addSink(new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper()));



        //********Join con Flusso depth != 1 ******

        DataStream<Tuple2<String, Double>> rankComment = timestampedAndWatermarked
                .filter(log -> log.getDepth() != 1)
                .flatMap(new Level3RedisMapper())
                .keyBy(0)
                .sum(1);



        // ********* Risultato finale (implementa formula) ************

        DataStream<String> classificaFinale = rankComment
                .union(rankLike)
                .keyBy(0)
                .sum(1)
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .apply(new AllWindowFunction<Tuple2<String, Double>, String, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable,
                                      Collector<String> collector) throws Exception {

                        List<Tuple2<String, Double>> tuple2s = new ArrayList<Tuple2<String, Double>>();
                        String res = "";
                        for (Tuple2<String, Double> my_tuple : iterable) {
                            tuple2s.add(new Tuple2<String, Double>(my_tuple.f0, my_tuple.f1));
                        }

                        Collections.sort(tuple2s, new Comparator<Tuple2<String, Double>>() {
                            @Override
                            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                                int v1 = (int) (o1.f1 * 100);
                                int v2 = (int) (o2.f1 * 100);
                                return v2 - v1;
                            }
                        });


                        // Date date_start = new Date(timeWindow.getStart());
                        res += " " + timeWindow.getStart()+" ,";
                        int size = tuple2s.size();
                        for (int i = 0; i < 10 && i < size; i++) {
                            res += " " +tuple2s.get(i).f0+" ,";
                            res += " " + String.format("%.2f", tuple2s.get(i).f1)+" ,";
                        }

                        res = res.substring(0, res.length() - 1);

                        collector.collect(res);

                        //TODO da rimuovere la stampa
                        System.out.println(res);

                    }
                });



        classificaFinale
                .writeAsText(Constants.QUERY3_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }

    private static Double computeNumLike(Integer num, Boolean isSelected){
        double w = 1;
        if(isSelected) w = 1.1;
        return  num * w * 0.3;

    }

}