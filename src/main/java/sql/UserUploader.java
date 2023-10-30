package sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import com.opencsv.CSVReader;

public class UserUploader {

    public static void uploadUsers(ErrorCollector errorCollector,int batchNum,CSVReader ur) throws IOException {
        try {
            ProgressBar progressBar;
            long total;
            Connection con = DatabaseConnectionPool.getConnection();
            total = Files.lines(Paths.get("data\\users.csv")).count();
            progressBar = new ProgressBar(total);
            PreparedStatement user_info = con
                    .prepareStatement("insert into user_info(user_id, name, sex, birthmonth, birthday, sign)"
                            + "values(?,?,?,?,?,?)");
            PreparedStatement user_role = con.prepareStatement("insert into user_role(user_id, level, role)"
                    + "values(?,?,?)");
            PreparedStatement user_following = con
                    .prepareStatement("insert into user_following(followeruserid, followinguserid)"
                            + "values(?,?)");
            while (ur.peek() != null) {
                ArrayList<User> users = Reader.readUsers(batchNum, errorCollector, ur);

                for (User i : users) {
                    if (User.check(i)) {
                        uploadUser(i, errorCollector, user_info, user_role, user_following);
                    }
                }
                user_info.executeBatch();
                user_role.executeBatch();
                user_following.executeBatch();
                System.out.printf("%d lines read.", progressBar.getcurrent());
                progressBar.update(batchNum);
            }
            progressBar.end();
            ur.close();
            con.commit();
            errorCollector.displayErrors();
        } catch (SQLException e) {
            System.out.println(e.toString());
        }
    }
    private static void uploadUser(User user, ErrorCollector errorCollector, PreparedStatement user_info,
			PreparedStatement user_role, PreparedStatement user_following) {
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
			// errorCollector.addError(e.toString());
			System.out.println(e.toString());
		}
	}
}
