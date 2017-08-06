package com.sohu.news.hbaseUtil;

/**
 * Created by T5 on 2016/11/15.
 */

        //import com.sohu.adrd.newsRecommend.online.updateTag.Constants;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

//import redis.util.Key;

/**
 */
public class JedisClusterService {

    private static Logger LOGGER = LoggerFactory.getLogger(JedisClusterService.class);

    private String servers;
    //private int timeout = Constants.jedisClusterTimeout();
    private int timeout = 5000;
    //private int maxRedirections = Constants.maxRedirections();
    private int maxRedirections = 1000;
    private Set<HostAndPort> hostAndPorts;
    private JedisCluster jedisCluster;
    private Boolean locked = false;

    public JedisClusterService(String servers){
        if (StringUtils.isBlank(servers)) {
            LOGGER.warn("redis servers is empty! ");
            System.exit(-1);
        }
//        Jedis璁块棶redis闆嗙兢
        hostAndPorts = new HashSet<HostAndPort>();
        String[] sers = servers.split(",");
        for (String ser : sers) {
            String[] hostAndPort = ser.split(":");
            if (hostAndPort.length != 2) {
                LOGGER.warn("hostAndPort error! addr = {}", ser);
                continue;
            }
            String host = hostAndPort[0];
            int port = Integer.parseInt(hostAndPort[1]);
            hostAndPorts.add(new HostAndPort(host, port));
        }
        if (hostAndPorts.isEmpty()) {
            LOGGER.warn("hostAndPorts is empty! ");
            System.exit(-1);
        }
        jedisCluster = new JedisCluster(hostAndPorts,timeout,maxRedirections);
    }

    /**
     * 閲嶆柊鑾峰緱杩炴帴
     */
    public synchronized void reConnect(){
        try {
            try {
                //Thread.sleep(Constants.jedisExceptionSleep());
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            //if(jedisCluster.exists(Constants.redisAliveKey())){
            if(jedisCluster.exists("redis_alive")){
                System.out.println("alive");
                return;
            }
            //jedisCluster.close();
            jedisCluster = new JedisCluster(hostAndPorts, timeout,maxRedirections);
        }catch (Exception e){
            reConnect();
        }
    }

    public JedisClusterService(String servers, int timeout){
        this.servers = servers;
        this.timeout = timeout;
        if (StringUtils.isBlank(servers)) {
            LOGGER.warn("redis servers is empty! ");
            System.exit(-1);
        }
//        Jedis璁块棶redis闆嗙兢
        hostAndPorts = new HashSet<HostAndPort>();
        String[] sers = servers.split(",");
        for (String ser : sers) {
            String[] hostAndPort = ser.split(":");
            if (hostAndPort.length != 2) {
                LOGGER.warn("hostAndPort error! addr = {}", ser);
                continue;
            }
            String host = hostAndPort[0];
            int port = Integer.parseInt(hostAndPort[1]);
            hostAndPorts.add(new HostAndPort(host, port));
        }
        if (hostAndPorts.isEmpty()) {
            LOGGER.warn("hostAndPorts is empty! ");
            System.exit(-1);
        }
        jedisCluster = new JedisCluster(hostAndPorts,timeout,maxRedirections);
    }


    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    /* redis open functions */

    /**
     * delete key from redis cluster
     *
     * @param key
     * @return
     */
//    public long del(Key key) {
//        long result = jedisCluster.del(key.toString());
//        return result;
//    }

    public long del(String key) {
        long result = jedisCluster.del(key.toString());
        return result;
    }


    /**
     * get key's value from redis
     * @param key
     * @return
     */
//    public String get(Key key) {
//        try {
//            String result = jedisCluster.get(key.toString());
//            return result;
//        }catch (Exception e){
//            LOGGER.error("expire error e = {}" , e);
//            try {
//                Thread.sleep(Constant.jedisExceptionSleep);
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//            this.reConnect();
//            return get(key);
//        }
//    }

    public String get(String key) {
        try {
            String result = jedisCluster.get(key);
            return result;
        }catch (Exception e){
            LOGGER.error("expire error e = {}" , e);
            try {
                //Thread.sleep(Constants.jedisExceptionSleep());
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            this.reConnect();
            return get(key);
        }
    }

    /**
     * set key's expire time
     *
     * @param key
     * @param seconds
     * @return
     * 瀵瑰簲璁烘枃涓細
     * 鐢变簬姣忎釜event_id閮藉叿鏈夋椂闂存埑锛孖dRegistry鍙互鎹鍒犻櫎閭ｄ簺瓒呰繃N澶╃殑id銆傛垜浠皢N绉颁负鍨冨溇鍥炴敹闃堝??
     */
//    public long expire(Key key, int seconds) {
//        try {
//            long result = jedisCluster.expire(key.toString(), seconds);
//            return result;
//        }catch (Exception e){
//            LOGGER.error("expire error e = {}" , e);
//            try {
//                Thread.sleep(Constant.jedisExceptionSleep);
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//            this.reConnect();
//            return expire(key, seconds);
//        }
//
//    }

    public long expire(String key, int seconds) {
        try {
            long result = jedisCluster.expire(key, seconds);
            return result;
        }catch (Exception e){
            LOGGER.error("expire error e = {}" , e);
            try {
                //Thread.sleep(Constants.jedisExceptionSleep());
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            this.reConnect();
            return expire(key, seconds);
        }

    }


    /**
     * set key's expire time at future unix time,
     * @param key
     * @param seconds
     * @return
     */
//    public long expireAt(Key key, int seconds) {
//        long result = jedisCluster.expireAt(key.toString(), seconds);
//        return result;
//    }

    /**
     * set value if key not exist
     * @param key
     * @param object
     * @return 锛?1琛ㄧず璁剧疆鎴愬姛锛屽惁鍒?0銆?
     * sentnx鍛戒护锛氬鏋滄寚瀹氱殑Key涓嶅瓨鍦紝鍒欒瀹氳Key鎸佹湁鎸囧畾瀛楃涓睼alue锛屾鏃跺叾鏁堟灉绛変环浜嶴ET鍛戒护銆?
     * 鐩稿弽锛屽鏋滆Key宸茬粡瀛樺湪锛岃鍛戒护灏嗕笉鍋氫换浣曟搷浣滃苟杩斿洖銆?
     * 闇?瑕佸姞閿佷互閬垮厤澶氫釜瀹㈡埛绔悓鏃惰繘琛岀紦瀛橀噸寤?(涔熷氨鏄涓鎴风锛屽悓涓?鏃堕棿杩涜鎿嶄綔锛?
     */
//    public long setNotExist(Key key, Object object) {
//        long result = jedisCluster.setnx(key.toString(), object.toString());
//        return result;
//    }


    /**
     * set value if key not exist
     * @param key
     * @param object
     * @return 锛氭?绘槸杩斿洖"OK"
     * set鍛戒护锛氳瀹氳Key鎸佹湁鎸囧畾鐨勫瓧绗︿覆Value锛屽鏋滆Key宸茬粡瀛樺湪锛屽垯瑕嗙洊鍏跺師鏈夊??
     */
//    public String set(Key key, Object object) {
//        String result = jedisCluster.set(key.toString(), object.toString());
//        return result;
//    }

    public String set(String key, Object object) {
        String result = jedisCluster.set(key, object.toString());
        return result;
    }

    /**
     * set hash value if key not exist
     * @param key
     * @param field
     * @param object
     * @return 锛?1琛ㄧず鏂扮殑Field琚缃簡鏂板?硷紝0琛ㄧずKey鎴朏ield宸茬粡瀛樺湪锛岃鍛戒护娌℃湁杩涜浠讳綍鎿嶄綔
     * hsetnx锛氬彧鏈夊綋鍙傛暟涓殑Key鎴朏ield涓嶅瓨鍦ㄧ殑鎯呭喌涓嬶紝涓烘寚瀹氱殑Key璁惧畾Field/Value瀵癸紝
     * 鍚﹀垯璇ュ懡浠や笉浼氳繘琛屼换浣曟搷浣?
     */
//    public long hashSetNotExist(Key key, String field, Object object) {
//        long result = jedisCluster.hsetnx(key.toString(), field, object.toString());
//        return result;
//    }

    /**
     * set hash value
     * @param key
     * @param field
     * @param object
     * @return
     */
//    public long hashSet(Key key, String field, Object object) {
//        long result = jedisCluster.hset(key.toString(), field, object.toString());
//        return result;
//    }
//
//    public List<String> hashmGet(Key key,String...fields){
//        try {
//            return jedisCluster.hmget(key.toString(), fields);
//        }catch(Exception e){
//            LOGGER.error("hashmget error e = {}" , e);
//            try {
//                Thread.sleep(Constant.jedisExceptionSleep);
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//            this.reConnect();
//            return hashmGet(key,fields);
//        }
//    }

    /**
     * get hash value
     * @param key
     * @param field
     * @return
     */
//    public String hashGet(Key key, String field) {
//        String result = jedisCluster.hget(key.toString(), field);
//        return result;
//    }

    public String hashGet(String key, String field) {
        String result = jedisCluster.hget(key, field);
        return result;
    }

    /**
     *
     * @param key
     * @return  :Field/Value鐨勫垪琛?
     * 鑾峰彇璇ラ敭鍖呭惈鐨勬墍鏈塅ield/Value銆傚叾杩斿洖鏍煎紡涓轰竴涓狥ield銆佷竴涓猇alue锛屽苟浠ユ绫绘帹
     */
//    public Map<String, String> hashGetAll(Key key) {
//        try {
//            return jedisCluster.hgetAll(key.toString());
//        }catch (Exception e){
//            LOGGER.error("hashgetall error e = {}" , e);
//            try {
//                Thread.sleep(Constant.jedisExceptionSleep);
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//            this.reConnect();
//            return hashGetAll(key);
//        }
//    }

    public Map<String, String> hashGetAll(String key) {
        try {
            return jedisCluster.hgetAll(key);
        }catch (Exception e){
            LOGGER.error("hashgetall error e = {}" , e);
            try {
                //Thread.sleep(Constants.jedisExceptionSleep());
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            this.reConnect();
            return hashGetAll(key);
        }
    }

    /**
     * del hash value
     * @param key
     * @param field
     * @return
     */
//    public long hashDel(Key key, String field) {
//        long result = jedisCluster.hdel(key.toString(), field);
//        return result;
//    }

    public long hashDel(String key,String field){
        try {
            long result = jedisCluster.hdel(key, field);
            return result;
        }catch (Exception e){
            LOGGER.error("hashdel error e = {}" , e);
            try {
                //Thread.sleep(Constants.jedisExceptionSleep());
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            this.reConnect();
            return hashDel(key, field);
        }
    }

    /**
     * multi hash set
     * @param key
     * @param kvs
     * @return
     * 閫愬渚濇璁剧疆鍙傛暟涓粰鍑虹殑Field/Value瀵广?傚鏋滃叾涓煇涓狥ield宸茬粡瀛樺湪锛?
     * 鍒欑敤鏂板?艰鐩栧師鏈夊?笺?傚鏋淜ey涓嶅瓨鍦紝鍒欏垱寤烘柊Key锛屽悓鏃惰瀹氬弬鏁颁腑鐨凢ield/Value
     */
//    public String hashPutAll(Key key, Map<String, String> kvs) {
//        String result = jedisCluster.hmset(key.toString(), kvs);
//        return result;
//    }

    public String hashPutAll(String key, Map<String, String> kvs) {
        String result = jedisCluster.hmset(key.toString(), kvs);
        return result;
    }

    /**
     * push object on left side to list
     * @param key
     * @param object
     * @return:鎻掑叆鍚庨摼琛ㄤ腑鍏冪礌鐨勬暟閲?
     * 鍦ㄦ寚瀹欿ey鎵?鍏宠仈鐨凩ist Value鐨勫ご閮ㄦ彃鍏ュ弬鏁颁腑缁欏嚭鐨勬墍鏈塚alues銆?
     * 濡傛灉璇ey涓嶅瓨鍦紝璇ュ懡浠ゅ皢鍦ㄦ彃鍏ヤ箣鍓嶅垱寤轰竴涓笌璇ey鍏宠仈鐨勭┖閾捐〃锛屼箣鍚庡啀灏嗘暟鎹粠閾捐〃鐨勫ご閮ㄦ彃鍏?
     */
//    public long lpush(Key key, Object object) {
//        long result = jedisCluster.lpush(key.toString(), object.toString());
//        return result;
//    }

    /**
     * push objects by batch
     * @param key
     * @param objects
     * @return
     */
//    public long lpushAll(Key key, Collection<?> objects) {
//        long result = jedisCluster.lpush(key.toString(), objects.toArray(new String[]{}));
//        return result;
//    }

    /**
     * pop object from right of list
     * @param key
     * @return
     * 杩斿洖骞跺脊鍑烘寚瀹欿ey鍏宠仈鐨勯摼琛ㄤ腑鐨勬渶鍚庝竴涓厓绱狅紝鍗冲熬閮ㄥ厓绱?
     */
//    public String rpop(Key key) {
//        String result = jedisCluster.rpop(key.toString());
//        return result;
//    }

    /**
     * push data from right side
     * @param key
     * @param object
     * @return
     * 鍦ㄦ寚瀹欿ey鎵?鍏宠仈鐨凩ist Value鐨勫熬閮ㄦ彃鍏ュ弬鏁颁腑缁欏嚭鐨勬墍鏈塚alues
     */
//    public long rpush(Key key, Object object) {
//        long result = jedisCluster.rpush(key.toString(), object.toString());
//        return result;
//    }


    /**
     * get datas from start to end, if end = -1, means last element
     * @param key
     * @param start
     * @param end
     * @return
     */
//    public List<String> lrange(Key key, long start, long end) {
//        List<String> result = jedisCluster.lrange(key.toString(), start, end);
//        return result;
//    }

    /**
     * reindex of list to certain size, remain offset between start and end
     * @param key
     * @param start
     * @param end
     * @return
     * 璇ュ懡浠ゅ皢浠呬繚鐣欐寚瀹氳寖鍥村唴鐨勫厓绱狅紝浠庤?屼繚璇侀摼鎺ヤ腑鐨勫厓绱犳暟閲忕浉瀵规亽瀹氥??
     * start鍜宻top鍙傛暟閮芥槸0-based锛?0琛ㄧず澶撮儴鍏冪礌銆?
     * 鍜屽叾浠栧懡浠や竴鏍凤紝start鍜宻top涔熷彲浠ヤ负璐熷?硷紝-1琛ㄧず灏鹃儴鍏冪礌銆傚鏋渟tart澶т簬閾捐〃鐨勫熬閮紝
     * 鎴杝tart澶т簬stop锛岃鍛戒护涓嶉敊鎶ラ敊锛岃?屾槸杩斿洖涓?涓┖鐨勯摼琛紝涓庢鍚屾椂璇ey涔熷皢琚垹闄ゃ??
     * 濡傛灉stop澶т簬鍏冪礌鐨勬暟閲忥紝鍒欎繚鐣欎粠start寮?濮嬪墿浣欑殑鎵?鏈夊厓绱?
     */
//    public String ltrim(Key key, long start, long end) {
//        String result = jedisCluster.ltrim(key.toString(), start, end);
//        return result;
//    }

    /**
     * remove data from list, if count = 0, remove all, if count = 1 or -1, just remove from certain side
     * @param key
     * @param count
     * @param object
     * @return
     * 鍦ㄦ寚瀹欿ey鍏宠仈鐨勯摼琛ㄤ腑锛屽垹闄ゅ墠count涓?肩瓑浜巚alue鐨勫厓绱犮??
     * 濡傛灉count澶т簬0锛屼粠澶村悜灏鹃亶鍘嗗苟鍒犻櫎锛屽鏋渃ount灏忎簬0锛屽垯浠庡熬鍚戝ご閬嶅巻骞跺垹闄ゃ??
     * 濡傛灉count绛変簬0锛屽垯鍒犻櫎閾捐〃涓墍鏈夌瓑浜巚alue鐨勫厓绱犮?傚鏋滄寚瀹氱殑Key涓嶅瓨鍦紝鍒欑洿鎺ヨ繑鍥?0銆?
     */
//    public long lrem(Key key, long count, Object object) {
//        long result = jedisCluster.lrem(key.toString(), count, object.toString());
//        return result;
//    }


    /**
     * incr a key by one
     * @param key
     * @return
     * 灏嗘寚瀹欿ey鐨刅alue鍘熷瓙鎬х殑閫掑1銆傚鏋滆Key涓嶅瓨鍦紝鍏跺垵濮嬪?间负0锛屽湪incr涔嬪悗鍏跺?间负1
     */
//    public long incr(Key key) {
//        long result = jedisCluster.incr(key.toString());
//        return result;
//    }

    public long incr(String key){
        long result = jedisCluster.incr(key);
        return result;
    }


    /**
     * incr a key by value
     * @param key
     * @param value
     * @return
     */
//    public long incrByValue(Key key, int value) {
//        long result = jedisCluster.incrBy(key.toString(), value);
//        return result;
//    }


//    public long hashIncr(Key key, String field) {
//        return hashIncr(key, field, 1);
//    }
//
//    public long hashIncr(Key key, String field, int val) {
//        try {
//            long result = jedisCluster.hincrBy(key.toString(), field, val);
//            return result;
//        }catch (Exception e){
//            LOGGER.error("hashIncr error e = {}" , e);
//            try {
//                Thread.sleep(Constant.jedisExceptionSleep);
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//            this.reConnect();
//            return hashIncr(key, field, val);
//        }
//    }

    /**
     * query key ttl
     * @param key
     * @return: 杩斿洖鎵?鍓╂弿杩帮紝濡傛灉璇ラ敭涓嶅瓨鍦ㄦ垨娌℃湁瓒呮椂璁剧疆锛屽垯杩斿洖-1
     * 鑾峰彇璇ラ敭鎵?鍓╃殑瓒呮椂鎻忚堪
     */
//    public long ttl(Key key) {
//        long result = jedisCluster.ttl(key.toString());
//        return result;
//    }
//
    public long ttl(String key) {
        long result = jedisCluster.ttl(key);
        return result;
    }

    public void close(){
//        this.jedisCluster.shutdown();
    }


//    public boolean exist(Key key) {
//        return jedisCluster.exists(key.toString());
//    }
}
