package jdbc;

import conf.ConfigurationManager;
import contains.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;

/**
 * Created by wangGuoGuang&&tianZhongQiu
 * JDBC 辅助组件
 */
//06 单例模式的好处：实例只有一个，则保证了数据库连接只有一个，避免重复创建连接。JDBC类不能有硬编码
public class JDBCHelper {



    /**
     * 第一步：在静态代码块中，直接加载数据库的驱动
     */

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 第二步，实现JDBCHelper的单例化
     */
    private static JDBCHelper instance = null;

    /**
     * 获取单例
     * @return 单例
     */
    public static JDBCHelper getInstance() {
        if(instance == null) {
            synchronized(JDBCHelper.class) {
                if(instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    // 数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     * 第三步：实现单例的过程中，创建唯一的数据库连接池
     */
    private JDBCHelper() {
        // 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
        // 这个，可以通过在配置文件中配置的方式，来灵活的设定
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        // 然后创建指定数量的数据库连接，并放入数据库连接池中
        for(int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第四步，提供获取数据库连接的方法
     */
    public synchronized Connection getConnection() {
        while(datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     *
     * 第五步：开发增删改查的方法
     * 1、执行增删改SQL语句的方法
     * 2、执行查询SQL语句的方法
     * 3、批量执行SQL语句的方法
     */
    /**
     * 执行增删改SQL语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params,Connection conns) {
        int rtn = 0;
        Connection conn = conns;
        PreparedStatement ps = null;

        try {
//            conn = getConnection();
            ps = conn.prepareStatement(sql);

            for(int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }

            rtn = ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for(int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            //executeQuery()返回的是一个结果集
            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }

}

