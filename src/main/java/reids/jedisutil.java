package reids;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

/**
 * 读取指定目录下的所有文件中的key设置失效时间
 * @author Administrator
 *
 */
public class jedisutil {
	// redis ip地址
	private static String redis_ip = "";
	// redis 端口号
	private static int redis_prot = 0;
	// redis keys文件夹路径
	private static String kyes_path = "";
	// 日志输出路径
	private static String log_path = "";
	//　redis key前缀
	private static String key_prefix = "";
	// 失效时间(秒)
	private static int invalid_time = 0;
	
	private static JedisPool pool = null; 
	
	/**
	 * 获取配置文件中的相关配置
	 */
	static {
		// 获取配置文件路径
		String path = System.getProperty("user.dir");
		String file = path + "/config.properties";
		file = file.replaceAll("\\\\", "/");
		Properties pro = new Properties();
		try {
			pro.load(new  FileInputStream(new File(file)));
			redis_ip = pro.getProperty("redis_ip");
			redis_prot = Integer.parseInt(pro.getProperty("redis_prot"));
			kyes_path = pro.getProperty("keys_path").replaceAll("\\\\", "/");
			log_path = pro.getProperty("log_path").replaceAll("\\\\", "/");
			key_prefix = pro.getProperty("key_prefix");
			invalid_time = Integer.parseInt(pro.getProperty("invalid_time"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 构建redis连接池
	 * @return
	 */
	public static JedisPool getPool() {
        if (pool == null) {  
            JedisPoolConfig config = new JedisPoolConfig();  
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；  
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
			config.setMaxTotal(30);
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(5);  
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；  
            config.setMaxWaitMillis(1000 * 15);
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；  
            config.setTestOnBorrow(true);  
            pool = new JedisPool(config, redis_ip, redis_prot);  
        }  
        return pool;  
    }
	
	/**
	 * 返还到连接池
	 * @param pool
	 * @param redis
	 */
	public static void returnResource(JedisPool pool, Jedis redis) {
		if (redis != null) {
			pool.returnResource(redis);
		}
	}
	
	/**
	 * 设置key有效时间
	 * @param key
	 * @param seconds
	 */
	public static void expire(String key, int seconds){
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();
			jedis.expire(key, seconds);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			e.printStackTrace();
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}
	
	/**
	 * 获取指定目录下的所有文件
	 * @param resource
	 * @return
	 */
	public static File[] getFiles(String resource) {
		File file = new File(resource);
		return file.listFiles();
	}
	
	/**
	 * 向指定文件中追加内容，若文件不存在则先创建文件
	 * @param fileName
	 * @param content
	 */
	public static void appendFile(String fileName, String content) {
		FileWriter writer = null;
		try {
			// 若文件不存在，则先创建文件
			File file = new File(fileName);
			if (!file.exists())
				file.createNewFile();
			// 采用在文件尾部追加的方式写文件
			writer = new FileWriter(fileName, true);
			writer.write(content);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 读取文件并设置失效时间，设置redis key 失效时间
	 * @param filePath
	 * @return key数量
	 */
	public static int readFile(String filePath) {
		int sum = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filePath))));
			String key = "";
			while ((key = br.readLine()) != null) {
				if (key.startsWith(key_prefix)) {
					System.out.println("io: " + key);
					expire(key, invalid_time);
					sum ++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return sum;
	}
	
	/**
	 * 扫描方式读取文件，设置redis key 失效时间
	 * @param filePath
	 * @return key数量
	 */
	public static int scannerReadFile(String filePath) {
		int sum = 0;
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(filePath);
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNext()) {
				String key = sc.nextLine();
				if (key.startsWith(key_prefix)) {
					System.out.println("scanner: " + key);
					expire(key, invalid_time);
					sum ++;
				}
			}
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
		} catch (IOException e){
			e.printStackTrace();
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
				if (sc != null)
					sc.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return sum;
	}
	
	/**
	 * 迭代方式读取文件，设置redis key 失效时间
	 * @param filePath
	 * @return key数量
	 * @throws Exception 
	 */
	public static int lineIteratorReadFile (String filePath) {
		int sum = 0;
		LineIterator it = null;
		try {
			it = FileUtils.lineIterator(new File(filePath), "UTF-8");
			while (it.hasNext()) {
				String key = it.nextLine();
				if (key.startsWith(key_prefix)) {
					System.out.println("iterator: " + key);
					expire(key, invalid_time);
					sum ++;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return sum;
		} finally {
			LineIterator.closeQuietly(it);
		}
		return sum;
	}
	
	public static void main(String[] args) {
		int sum = 0;
		// 根据存放keys的文件夹路径获取文件夹下的所有key文件
		File[] files = getFiles(kyes_path);
		if (files != null) {
			for (File file : files) {
				String fileName = file.getName();
				String folderPath = kyes_path + fileName;
				try {
					if (file.isFile()) {
						appendFile(log_path, getTime(System.currentTimeMillis()) + " " + fileName + ":开始设置失效时间...\r\n");
						// 1. 消耗内存最大
//						sum = readFile(folderPath);
						// 2. 消耗内存较小
//						sum = scannerReadFile(folderPath);
						// 3. 消耗内存最小
						sum = lineIteratorReadFile(folderPath);
						// 处理完文件后删除处理成功文件
						file.delete();
					}
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					appendFile(log_path, getTime(System.currentTimeMillis()) + " " + fileName + ":设置失效时间完成,共设置 " + sum + " 个key\r\n\r\n");
				}
			}
		} else {
			System.out.println(kyes_path + "目录不存在或为空!");
		}
	}
	
	/**
	 * 根据时间戳获取时间
	 * @param time
	 * @return
	 */
	public static String getTime(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(time));
	}
	
	/**
	 * 获取项目路径
	 * @return
	 */
	public static String getPath() {
		return System.getProperty("user.dir");
	}
	
}
