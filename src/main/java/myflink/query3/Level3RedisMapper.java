package myflink.query3;

import myflink.entity.CommentLog;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public  class Level3RedisMapper extends RichFlatMapFunction<CommentLog, Tuple2<String, Double>> {

    boolean docker = false;
    public Level3RedisMapper() {

    }

    @Override
    public void flatMap(CommentLog commentLog, Collector<Tuple2<String, Double>> collector) throws Exception {

        //Jedis jedis = null;
        try(Jedis jedis = JedisPoolHolder.getInstance().getResource()){
        //try{

            //if(docker) {jedis = new Jedis("redis", 6379);}
            //else{jedis = new Jedis();}
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
        /*
        finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        */
    }
}
