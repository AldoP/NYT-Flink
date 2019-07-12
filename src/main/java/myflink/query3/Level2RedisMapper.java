package myflink.query3;

import myflink.entity.CommentLog;
import myflink.utils.JedisPoolHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public  class Level2RedisMapper implements MapFunction <CommentLog, Tuple2<String, String>> {

    public Level2RedisMapper() {
    }

    @Override
    public Tuple2<String, String> map(CommentLog commentLog) throws Exception {

        Tuple2<String, String> result = null;

        //try (Jedis jedis = jedisPool.getResource()) {
        try (Jedis jedis = JedisPoolHolder.getInstance().getResource()) {
            String commentID = commentLog.getCommentID();
            String userID = commentLog.getUserID();
            String inReplyTo = commentLog.getInReplyTo();

            String userIDPadre = jedis.get(inReplyTo);

            result = new Tuple2<String, String>(commentID, userID);

            if(userIDPadre != null)
                result =  new Tuple2<>(commentID, (userID+";"+userIDPadre));

        }
        catch (Exception e){
            System.err.println("Errore mapper livello 2, "+e.toString());
        }

        return result;


    }
}
