package myflink.query3;

import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import redis.clients.jedis.Jedis;

public  class Level2RedisMapper extends RichMapFunction<Tuple3<String, String, String>, Tuple2<String, String>> {

    public Level2RedisMapper() {}

    @Override
    public Tuple2<String, String> map(Tuple3<String, String, String> myTuple) throws Exception {

        Tuple2<String, String> result = null;
        Jedis jedis = null;
        //try (Jedis jedis = new Jedis("redis", 6379)) {
        try {
            //jedis = JedisPoolHolder.getInstance().getResource();
            jedis = new Jedis("redis", 6379);
            String commentID = myTuple.f0; // commentLog.getCommentID();
            String userID = myTuple.f1; //commentLog.getUserID();
            String inReplyTo = myTuple.f2; // commentLog.getInReplyTo();
            String userIDPadre = null;

            try {
                userIDPadre = jedis.get(inReplyTo);
            }
            catch (Exception e){
                System.err.println("errore jedis get in level2RedisMapper"+e);
            }

            result = new Tuple2<String, String>(commentID, userID);

            if(userIDPadre != null)
                result =  new Tuple2<>(commentID, (userID+";"+userIDPadre));



        }
        catch (Exception e){
            System.err.println("Errore mapper livello 2, "+e.toString());
        }
        finally {
            if (jedis != null) {
                jedis.close();
            }
        }


        return result;


    }
}
