package sql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Map.Entry;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class single {
	private static Connection con = null;
	private static PreparedStatement stmt = null;
	private static PreparedStatement user_info;
	private static PreparedStatement user_role;
	private static PreparedStatement user_following;
	private static PreparedStatement video_info;
	private static PreparedStatement video_action;
	private static PreparedStatement video_status;
	private static PreparedStatement video_view;
	private static CSVReader ur;
	private static CSVReader vr;
	public static ErrorCollector errorCollector = new ErrorCollector();
	final static int BATCHNUM = 10;

	public static void main(String[] args) throws SQLException, CsvValidationException, IOException {

		initialization();
		ProgressBar progressBar;
		long total;
		total = Files.lines(Paths.get("data\\users.csv")).count();
		progressBar = new ProgressBar(total);
		while (ur.peek() != null) {
			ArrayList<User> users = Reader.readUsers(BATCHNUM, errorCollector, ur);

			for (User i : users) {
				if (User.check(i)) {
					uploadUser(i, errorCollector);
				}
			}
			user_info.executeBatch();
			user_role.executeBatch();
			user_following.executeBatch();
			System.out.printf("%d lines read.", progressBar.getcurrent());
			progressBar.update(BATCHNUM);
		}
		progressBar.end();
		ur.close();
		con.commit();
		errorCollector.displayErrors();

		total = Files.lines(Paths.get("data\\videos.csv")).count();
		progressBar = new ProgressBar(total);
		while (vr.peek() != null) {
			ArrayList<Video> videos = Reader.readVideos(BATCHNUM, errorCollector, vr);

			for (Video i : videos) {
				uploadVideo(i, errorCollector);
			}
			video_info.executeBatch();
			video_action.executeBatch();
			video_status.executeBatch();
			video_view.executeBatch();
			System.out.printf("%d lines read.", progressBar.getcurrent());
			progressBar.update(BATCHNUM);
		}
		progressBar.end();
		vr.close();
		con.commit();
		errorCollector.displayErrors();

		closeDB();
	}

	private static void uploadUser(User user, ErrorCollector errorCollector) {
		try {
			user_info.setLong(1, user.id);
			user_info.setString(2, user.name);
			user_info.setString(3, String.valueOf(user.sex));
			if (user.birthmonth == 0 || user.birthday == 0) {
				user_info.setNull(4, java.sql.Types.INTEGER);
				user_info.setNull(5, java.sql.Types.INTEGER);
			} else {
				user_info.setInt(4, user.birthmonth);
				user_info.setInt(5, user.birthday);
			}
			user_info.setString(6, user.sign);
			user_info.addBatch();
			user_role.setLong(1, user.id);
			user_role.setInt(2, user.level);
			user_role.setString(3, user.identity);
			user_role.addBatch();
			for (Long i : user.following) {
				user_following.setLong(1, user.id);
				user_following.setLong(2, i);
				user_following.addBatch();
			}
		} catch (SQLException e) {
			errorCollector.addError(e.toString());
		}
	}

	private static void uploadVideo(Video video, ErrorCollector errorCollector) {
		try {
			video_info.setString(1, video.id);
			video_info.setString(2, video.title);
			video_info.setString(3, video.description);
			video_info.setInt(4, video.duration);
			video_info.addBatch();
			video_status.setString(1, video.id);
			video_status.setLong(2, video.author_id);
			video_status.setTimestamp(3, Timestamp.valueOf(video.commit_time));
			video_status.setTimestamp(4, Timestamp.valueOf(video.review_time));
			video_status.setTimestamp(5, Timestamp.valueOf(video.public_time));
			video_status.setLong(6, video.reviewer_id);
			video_status.addBatch();
			if (video.view != null) {
				for (Entry<Long, Integer> i : video.view) {
					video_view.setString(1, video.id);
					video_view.setLong(2, i.getKey());
					video_view.setInt(3, i.getValue());
					video_view.addBatch();
				}
			} else {
				errorCollector.addError("View list is null");
			}
			if (video.like != null) {
				for (Long i : video.like) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "like");
					video_action.addBatch();
				}
			} else {
				System.out.println("Like list is null");
			}
			if (video.coin != null) {
				for (Long i : video.coin) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "coin");
					video_action.addBatch();
				}
			} else {
				System.out.println("Coin list is null");
			}
			if (video.favorite != null) {
				for (Long i : video.favorite) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "favorite");
					video_action.addBatch();
				}
			} else {
				System.out.println("Favorite list is null");
			}
		} catch (SQLException e) {
			errorCollector.addError(e.toString());
		}
	}

	private static void initialization() throws SQLException {

		Properties prop = new Properties();
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
			stmt.execute(
					"TRUNCATE TABLE user_info, user_role, user_following, video_info, video_action, video_status, video_view CASCADE");
			stmt.close();
		}
		con.setAutoCommit(false);

		user_info = con.prepareStatement("insert into user_info(user_id, name, sex, birthmonth, birthday, sign)"
				+ "values(?,?,?,?,?,?)");
		user_role = con.prepareStatement("insert into user_role(user_id, level, role)"
				+ "values(?,?,?)");
		user_following = con.prepareStatement("insert into user_following(followeruserid, followinguserid)"
				+ "values(?,?)");
		video_info = con.prepareStatement("insert into video_info(video_id, title, description, duration)"
				+ "values(?,?,?,?)");
		video_action = con.prepareStatement("insert into video_action(video_id, user_id, action)"
				+ "values(?,?,?)");
		video_status = con.prepareStatement(
				"insert into video_status(video_id, owner_id, create_time, review_time, public_time, reviewer_id)"
						+ "values(?,?,?,?,?,?)");
		video_view = con.prepareStatement("insert into video_view(video_id, user_id, view_time)"
				+ "values(?,?,?)");

		try {
			ur = new CSVReader(new FileReader("data\\users.csv"));
			ur.readNext();
			vr = new CSVReader(new FileReader("data\\videos.csv"));
			vr.readNext();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (CsvValidationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void openDB(String host, String dbname,
			String user, String pwd) {
		try {
			//
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
			System.exit(1);
		}
		String url = "jdbc:postgresql://" + host + "/" + dbname;
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
