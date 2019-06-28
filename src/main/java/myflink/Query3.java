package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;



public class Query3 {

    public static void main(String[] args) throws Exception {


        // Create the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // Get the input data by connecting the socket. Here it is connected to the local port 9000. If the port 9000 has been already occupied, change to another port.
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // Parse the data, and group, windowing and aggregate it by word.
        DataStream<CommentLog> data = text
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
                            out.collect(null);
                        }

                    }
                });


        DataStream<CommentLog> timestampedAndWatermarked = data
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog logIntegerTuple2) {
                        return logIntegerTuple2.getCreateDate();
                    }
                });

        /*
            ********* Numero di Like **********
         */
        DataStream<Tuple2<String, Double>> a = timestampedAndWatermarked
                .filter(log -> log.getDepth() == 1) // filtro i soli commenti diretti
                .map(myLog -> new Tuple2<String, Double>(
                        myLog.getUserID(),
                        computeNumLike (myLog.getRecommendations(), myLog.getEditorsSelection()))
                ).returns(Types.TUPLE(Types.STRING, Types.DOUBLE))// creo tuple (user_id, valore di a-iesimo
                .keyBy(stringDoubleTuple2 -> stringDoubleTuple2.f0)
                .timeWindow(Time.minutes(30))
                .sum(1); // sommo le a-i per ottenere la A di ogni utente

        // a.print().setParallelism(1);

        /*

               ********** Numero di commenti di risposta **********
         */

        // tabella da cui fare join
        DataStream<Tuple3<String, String, Integer>> joinTable = timestampedAndWatermarked
                .map(myLog -> new Tuple3<String, String, Integer>(
                        myLog.getUserID(),
                        myLog.getCommentID(),
                        myLog.getDepth())
                ).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));


        joinTable.print().setParallelism(2);

        DataStream<Tuple4<String, String, Long, Integer>> b = timestampedAndWatermarked

                .filter(new FilterFunction<CommentLog>() {
                    @Override
                    public boolean filter(CommentLog log) throws Exception {
                        return (!log.getInReplyTo().equals("0"));
                    }
                })

                .map(myLog -> new Tuple2<Long, String>(
                                myLog.getCreateDate(),
                                myLog.getInReplyTo()
                        )
                ).returns(Types.TUPLE(Types.LONG, Types.STRING))
                .join(joinTable)
                .where(new KeySelector<Tuple2<Long, String>, Object>() {

                    @Override
                    public Object getKey(Tuple2<Long, String> mytuple) throws Exception {
                        return mytuple.f1;
                    }
                })
                .equalTo(new KeySelector<Tuple3<String, String, Integer>, Object>() {

                    @Override
                    public Object getKey(Tuple3<String, String, Integer> mytuple) throws Exception {
                        return mytuple.f1;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new JoinFunction<Tuple2<Long, String>, Tuple3<String, String, Integer>, Tuple4<String, String, Long, Integer>>() {

                    @Override
                    public Tuple4<String, String, Long, Integer> join(Tuple2<Long, String> first,
                                                                      Tuple3<String, String, Integer> second) throws Exception {
                        String userId = second.f0;
                        String commentId = second.f1;
                        Long date = first.f0;
                        Tuple4<String, String, Long, Integer> tuple_out =
                                new Tuple4<String, String, Long, Integer>(userId, commentId, date, 1);
                        System.out.println("join: " + tuple_out);
                        return tuple_out;
                    }
                })
                // raggruppo per user ID
                .keyBy(0)
                // sommo gli uno per user ID
                .sum(3);


        b.print().setParallelism(1);

        env.execute("Socket Window Query 3");
        
    }

    private static Double computeNumLike(Integer num, Boolean isSelected){
        double w = 1;
        if(isSelected) w = 1.1;
        return  num * w;

    }
}
