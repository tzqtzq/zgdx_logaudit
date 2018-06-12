package contains;

/**
 * Created by tianzhongqiu on 2018/5/28.
 */
public interface Constants {

    String TYPE_HDFS="hdfs";
    String TYPE_HIVE="hive";
    String TYPE_YARN="yarn";

    String INDEX_HDFS="hdfs_logaudit";
    String INDEX_HIVE="hive_logaudit";
    String INDEX_YARN="yarn_logaudit";

    String TOPIC_HDFS="logauditHDFS";
    String TOPIC_HIVE="logauditHIVE";
    String TOPIC_YARN="logauditYARN";

    /**
     * Project Configuration Constants
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    int REPARTITION_COUNT = 10;

    /*
    shufflePartitionSize
     */
    String SIZE_REALTIME="8";
    String SIZE_OFFLINE="15";
}
