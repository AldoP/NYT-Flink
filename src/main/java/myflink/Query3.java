package myflink;

import myflink.utils.CommentLogSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import redis.clients.jedis.Jedis;

import java.util.*;

/*
        TODO: usare correttamente jedis, attualmente l'app dopo aver processato i dati del primo giorno fallisce
              con il seguente errore
    Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: redis.clients.jedis.exceptions.JedisConnectionException: java.net.SocketException: Connection reset
	at org.apache.flink.runtime.minicluster.MiniCluster.executeJobBlocking(MiniCluster.java:623)
	at org.apache.flink.streaming.api.environment.LocalStreamEnvironment.execute(LocalStreamEnvironment.java:123)
	at myflink.Query3.main(Query3.java:266)
Caused by: redis.clients.jedis.exceptions.JedisConnectionException: java.net.SocketException: Connection reset
	at redis.clients.util.RedisInputStream.ensureFill(RedisInputStream.java:201)



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

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();


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
        // Get the input data by connecting the socket. Here it is connected to the local port 9000. If the port 9000 has been already occupied, change to another port.
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");


        // Parse the data, and group, windowing and aggregate it by word.
        DataStream<CommentLog> timestampedAndWatermarked = text
                .flatMap(new FlatMapFunction<String, CommentLog>() {
                    @Override
                    public void flatMap(String value, Collector<CommentLog> out) {
                        try{
                            String[] values = value.split("\\s*,\\s*");
                            //generate obj
                            Long my_approveDate = Long.parseLong(values[0]) * 1000;
                            String my_articleID = values[1];
                            Integer my_articleWordCount = Integer.parseInt(values[2]);
                            String my_commentID = values[3];
                            String my_commentType = values[4];
                            Long my_createDate = Long.parseLong(values[5]) * 1000;
                            Integer my_depth = Integer.parseInt(values[6]);
                            Boolean my_editorsSelection = Boolean.parseBoolean(values[7]);
                            String my_inReplyTo = values[8];
                            String my_parentUserDisplayName = values[9];
                            Integer my_recommendations = Integer.parseInt(values[10]);
                            String my_sectionName = values[11];
                            String my_userDisplayName = values[12];
                            String my_userID = values[13];
                            String my_userLocation = values[14];

                            CommentLog myLog = new CommentLog(my_approveDate, my_articleID, my_articleWordCount, my_commentID,
                                    my_commentType, my_createDate, my_depth, my_editorsSelection, my_inReplyTo,
                                    my_parentUserDisplayName, my_recommendations, my_sectionName, my_userDisplayName,
                                    my_userID, my_userLocation);
                            out.collect(myLog);
                        }
                        catch (Exception e){
                            System.err.println("* Elemento cancellato *");
                        }

                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog logIntegerTuple2) {
                        return logIntegerTuple2.getCreateDate();
                    }
                });

        */
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
                .map(myData -> new Tuple2<String, Double>(

                        myData.f0,
                        (myData.f1*0.3))
                ).returns(Types.TUPLE(Types.STRING, Types.DOUBLE)); // sommo le a-i per ottenere la A di ogni utente
                 */

        // a.print().setParallelism(1);

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
                .map(new MapFunction<CommentLog, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(CommentLog commentLog) throws Exception {

                        Jedis jedis = new Jedis();

                        String commentID = commentLog.getCommentID();
                        String userID = commentLog.getUserID();
                        String inReplyTo = commentLog.getInReplyTo();

                        String userIDPadre = jedis.get(inReplyTo);

                        Tuple2<String, String> result = new Tuple2<String, String>(
                                commentID, userID);

                        if(userIDPadre != null)
                            result =  new Tuple2<>(commentID, (userID+";"+userIDPadre));

                        return result;

                    }

                })
                .addSink(new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper()));



        //********Join con Flusso depth != 1 ******

        DataStream<Tuple2<String, Double>> rankComment = timestampedAndWatermarked
                .filter(log -> log.getDepth() != 1)
                .flatMap(new FlatMapFunction<CommentLog, Tuple2<String, Double>>() {
                    @Override
                    public void flatMap(CommentLog commentLog, Collector<Tuple2<String, Double>> collector) throws Exception {

                        String commentIDPadre = commentLog.getInReplyTo();
                        // System.out.println("_"+commentIDPadre);
                        Jedis jedis = new Jedis();
                        String userIDPadre = jedis.get(commentIDPadre);
                        // System.out.println("_r: "+userIDPadre);

                        if (userIDPadre != null) {
                            // Join con il padre
                            if (!userIDPadre.contains(";")) {
                                collector.collect(new Tuple2<String, Double>(userIDPadre, 0.7));
                            }
                            // Join con il nonno
                            else {

                                String userIDNonno = userIDPadre.split(";")[1];
                                String userIDPadre2 = userIDPadre.split(";")[0];
                                collector.collect(new Tuple2<String, Double>(userIDPadre2, 0.7));
                                collector.collect(new Tuple2<String, Double>(userIDNonno, 0.7));

                            }
                        }

                    }
                })
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
}
