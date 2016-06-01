package recommend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bj58.dia.distribute.SimpleDistRedisClient;

public class RedisTool {
	private static final Logger logger = LoggerFactory.getLogger(RedisTool.class);
	private static SimpleDistRedisClient redis;
	public static boolean init(String[] nodes) {
		redis = new SimpleDistRedisClient();
		try {
			redis.init(nodes);
			logger.info("init redis success!!");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static SimpleDistRedisClient getRedisClient(){
		return redis;
	}
}
