package myflink;

import myflink.query3.Level2RedisMapper;
import myflink.query3.Level3RedisMapper;
import myflink.query3.MyRedisMapper;
import myflink.utils.CommentLogSchema;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.util.*;

/*
        TODO: sparapatate troppo lento

 */
public class Query3 {

    public static void main(String[] args) throws Exception {

        final int WINDOW_SIZE = 24; //in numero di ore

        // Create the execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // RocksDBStateBackend my_rocksDB = new RocksDBStateBackend("file:///tmp");
        // env.setStateBackend(my_rocksDB);

        JedisPoolHolder.init("localhost", 6379);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").build(); //aggiungere altri set


        // Get the input data
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<CommentLog> commentLog = env
                .addSource(new FlinkKafkaConsumer<>("test", new CommentLogSchema(), properties));

        // Assegna timestamp e watermark
        DataStream<CommentLog> timestampedAndWatermarked = commentLog
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
                .map(new Level2RedisMapper())
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
                        res += "\n--------------------";

                        Date date_start = new Date(timeWindow.getStart());
                        res += "TS: " + date_start;
                        int size = tuple2s.size();
                        for (int i = 0; i < 10 && i < size; i++) {
                            res += "\n User_" + (i + 1) + "  : " + tuple2s.get(i).f0;
                            res += "\n Rating_" + (i + 1) + "  : " + String.format("%.2f", tuple2s.get(i).f1);
                        }
                        res += "\n-------------------";
                        collector.collect(res);

                        //TODO da rimuovere la stampa
                        System.out.println(res);

                    }
                });



        classificaFinale
                .writeAsText(Constants.QUERY3_PATHOUT+"_"+WINDOW_SIZE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Socket Window Query 3");
        
    }

    private static Double computeNumLike(Integer num, Boolean isSelected){
        double w = 1;
        if(isSelected) w = 1.1;
        return  num * w * 0.3;

    }
/*
    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
*/
}
