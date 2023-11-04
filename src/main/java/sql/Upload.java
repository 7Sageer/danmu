package sql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class Upload {
	private static Connection con = null;
	private static PreparedStatement stmt = null;

	private static CSVReader ur, vr, dr;
	public static ErrorCollector errorCollector = new ErrorCollector();

	final static int THREADNUM = 20;
	static Properties prop = new Properties();
	static String url;

	public static void main(String[] args) throws SQLException, CsvValidationException, IOException {

		initialization();
		DatabaseConnectionPool.InitializeDBCPool();
		//UserUploader.uploadUsers(errorCollector, 500, ur);

		VideoUploader.uploadVideos(errorCollector, vr, THREADNUM, 10);

		//DanmuUploader.uploadDanmus(errorCollector, 5000, dr);

		closeDB();
	}

	

	private static void initialization() throws SQLException {

		try {
			FileInputStream fis = new FileInputStream("java\\sql\\src\\main\\java\\sql\\loader.cnf");
			prop.load(fis);
		} catch (IOException e) {
			System.err.println("No configuration file (loader.cnf) found");

		}

		String username = prop.getProperty("user");
		String password = prop.getProperty("password");
		String dbName = prop.getProperty("database");
		String host = prop.getProperty("host");
		openDB(host, dbName, username, password);
		Statement stmt;
		if (con != null) {
		stmt = con.createStatement();
		//stmt.execute("TRUNCATE danmu cascade");
		stmt.execute("truncate video_info,video_action, video_status, video_view CASCADE");
		//stmt.execute("truncate table user_info, user_role, user_following cascade");
		stmt.close();
		}
		con.setAutoCommit(false);
		con.commit();

		try {
			ur = new CSVReader(new InputStreamReader(new FileInputStream("data\\users.csv"), "UTF-8"));
			ur.readNext();
			vr = new CSVReader(new InputStreamReader(new FileInputStream("data\\videos.csv"), "UTF-8"));
			vr.readNext();
			dr = new CSVReader(new InputStreamReader(new FileInputStream("data\\danmu1.csv"), "UTF-8"));
			dr.readNext();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (CsvValidationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//这两个函数几乎被弃用了
	private static void openDB(String host, String dbname,
			String user, String pwd) {
		try {
			//
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
			System.exit(1);
		}
		url = "jdbc:postgresql://" + host + "/" + dbname;
		Properties props = new Properties();
		props.setProperty("user", user);
		props.setProperty("password", pwd);
		try {
			con = DriverManager.getConnection(url, props);
			System.out.println("Successfully connected to the database "
					+ dbname + " as " + user);
			con.setAutoCommit(false);
		} catch (SQLException e) {
			System.err.println("Database connection failed");
			System.err.println(e.getMessage());
			System.exit(1);
		}

	}

	private static void closeDB() {
		if (con != null) {
			try {
				if (stmt != null) {
					stmt.close();
				}
				con.close();
				con = null;
			} catch (Exception e) {
				// Forget about it
			}
		}
	}

}