package sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnectionPool {
    static Properties prop = new Properties();
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    private DatabaseConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public static void InitializeDBCPool() throws SQLException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("java\\sql\\src\\main\\java\\sql\\loader.cnf");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            if (fis != null)
                prop.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String username = prop.getProperty("user");
        String password = prop.getProperty("password");
        String dbName = prop.getProperty("database");
        String host = prop.getProperty("host");
        String url = "jdbc:postgresql://" + host + "/" + dbName;
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(20); // 最大连接数
        config.setMinimumIdle(2); // 最小空闲连接数
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setIdleTimeout(1000);
        config.setConnectionTimeout(0);
        ds = new HikariDataSource(config);
    }
}