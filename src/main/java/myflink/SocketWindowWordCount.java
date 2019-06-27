package myflink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

/*
    TODO: controllare se regge il carico dello spara patate
          salvare in output
 */

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {


        // Create the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // generate watermarks every 5 seconds
        //env.getConfig().setAutoWatermarkInterval(5000);

        // Get the input data by connecting the socket. Here it is connected to the local port 9000. If the port 9000 has been already occupied, change to another port.
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // Parse the data, and group, windowing and aggregate it by word.
        DataStream<Tuple2<Log, Integer>> data = text
                .flatMap(new FlatMapFunction<String, Tuple2<Log, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Log, Integer>> out) {
                        String[] values = value.split("\\s*,\\s*");
                        //generate obj
                        Long my_approveDate = Long.parseLong(values[0])*1000;
                        String my_articleID = values[1];
                        Integer my_articleWordCount = Integer.parseInt(values[2]);
                        String my_commentID = values[3];
                        String my_commentType = values[4];
                        Long my_createDate = Long.parseLong(values[5])*1000;
                        Integer my_depth = Integer.parseInt(values[6]);
                        Boolean my_editorsSelection = Boolean.parseBoolean(values[7]);
                        String my_inReplyTo = values[8];
                        String my_parentUserDisplayName = values[9];
                        Integer my_recommendations = Integer.parseInt(values[10]);
                        String my_sectionName = values[11];
                        String my_userDisplayName = values[12];
                        String my_userID = values[13];
                        String my_userLocation = values[14];

                        Log myLog = new Log(my_approveDate, my_articleID, my_articleWordCount, my_commentID,
                                my_commentType, my_createDate, my_depth, my_editorsSelection, my_inReplyTo,
                                my_parentUserDisplayName, my_recommendations, my_sectionName, my_userDisplayName,
                                my_userID, my_userLocation);
                        out.collect(Tuple2.of(myLog, 1));

                    }
                });


        DataStream<Tuple2<Log, Integer>> timestampedAndWatermarked = data
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Log, Integer>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple2<Log, Integer> logIntegerTuple2) {
                        return logIntegerTuple2.f0.getCreateDate();
                    }
                });

        // calcola quanto un articolo e' popolare con finestra sliding
        DataStream<Tuple3<String, Log, Integer>> my_sum = timestampedAndWatermarked
                .keyBy(value -> value.f0.articleID)
                .timeWindow(Time.seconds(5), Time.seconds(2))
                .sum(1)
                .map(new MapFunction<Tuple2<Log, Integer>, Tuple3<String, Log, Integer>>() {
                    @Override
                    public Tuple3<String, Log, Integer> map(Tuple2<Log, Integer> logIntegerTuple2) throws Exception {

                        return new Tuple3<>("label", logIntegerTuple2.f0, logIntegerTuple2.f1);
                    }
                });


        // printa la classifica con finestra tumbling
        DataStream<Tuple2<Log, Integer>> resultWind = my_sum
                .keyBy(0)
                .timeWindow(Time.seconds(2))
                .apply(new WindowFunction<Tuple3<String, Log, Integer>, Tuple2<Log, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow,
                                      Iterable<Tuple3<String, Log, Integer>> iterable,
                                      Collector<Tuple2<Log, Integer>> collector) throws Exception {

                        List<Tuple2<Integer, Log>> tuple2s = new ArrayList<Tuple2<Integer, Log>>();

                        for(Tuple3<String, Log, Integer> my_tuple : iterable){
                            tuple2s.add( new Tuple2<Integer, Log> (my_tuple.f2, my_tuple.f1));
                        }
                        Collections.sort(tuple2s, new Comparator<Tuple2<Integer, Log>>() {
                            @Override
                            public int compare(Tuple2<Integer, Log> o1, Tuple2<Integer, Log> o2) {
                                int v1 =  o1.f0.intValue();
                                int v2 =  o2.f0.intValue();
                                return v2-v1;
                            }
                        });

                        System.out.println("--------------------");
                        Date date_start = new Date(timeWindow.getStart());
                        Date date_end = new Date(timeWindow.getEnd());
                        System.out.println("size: "+tuple2s.size());
                        System.out.println("Time Window Start "+date_start.toString());
                        System.out.println("Time Window End: "+date_end.toString());
                        if(tuple2s.size() > 0){
                            System.out.println("\n First: "+tuple2s.get(0));
                        }
                        if(tuple2s.size() > 1){
                            System.out.println("\n Second: "+tuple2s.get(1));
                        }
                        if(tuple2s.size() > 2){
                            System.out.println("\n Third: "+tuple2s.get(2));
                        }

                        System.out.println("-------------------");
                    }
                });

        // Print the results to the console, note that here it uses the single-threaded printing instead of multi-threading
        resultWind.print().setParallelism(1);

        env.execute("Socket Window WordCount");

    }


}