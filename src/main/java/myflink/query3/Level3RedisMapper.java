package myflink.query3;

import myflink.entity.CommentLog;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public  class Level3RedisMapper implements FlatMapFunction<CommentLog, Tuple2<String, Double>> {

    public Level3RedisMapper() {

    }

    @Override
    public void flatMap(CommentLog commentLog, Collector<Tuple2<String, Double>> collector) throws Exception {

        try(Jedis jedis = JedisPoolHolder.getInstance().getResource()){

            String commentIDPadre = commentLog.getInReplyTo();
            String userIDPadre = jedis.get(commentIDPadre);

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
        catch (Exception e){
            System.err.println("Errore mapper livello 3, "+e.toString());
        }


    }
}
