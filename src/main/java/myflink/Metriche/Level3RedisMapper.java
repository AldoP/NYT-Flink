package myflink.Metriche;

import myflink.entity.CommentLog;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public  class Level3RedisMapper implements FlatMapFunction<Tuple2<CommentLog, Long>, Tuple3<String, Double, Long>> {


    public Level3RedisMapper() {

    }

    @Override
    public void flatMap(Tuple2<CommentLog, Long> commentLog, Collector<Tuple3<String, Double, Long>> collector) throws Exception {

        try(Jedis jedis = JedisPoolHolder.getInstance().getResource()){

            String commentIDPadre = commentLog.f0.getInReplyTo();
            String userIDPadre = jedis.get(commentIDPadre);

            if (userIDPadre != null) {
                // Join con il padre
                if (!userIDPadre.contains(";")) {
                    collector.collect(new Tuple3<String, Double, Long>(userIDPadre, 0.7, commentLog.f1 ));
                }
                // Join con il nonno
                else {

                    String userIDNonno = userIDPadre.split(";")[1];
                    String userIDPadre2 = userIDPadre.split(";")[0];
                    collector.collect(new Tuple3<String, Double, Long>(userIDPadre2, 0.7, commentLog.f1));
                    collector.collect(new Tuple3<String, Double, Long>(userIDNonno, 0.7, commentLog.f1));

                }
            }
        }
        catch (Exception e){
            System.err.println("Errore mapper livello 3, "+e.toString());
        }


    }
}
