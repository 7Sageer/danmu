package sql;
import java.util.Arrays;
import java.util.regex.*;

public class User {
    Long id;
    String name;
    char sex;
    int birthday, birthmonth;
    int level;
    String sign;
    long[] following;
    String identity;

    public User(String[] data, ErrorCollector errorCollector) {
        if (data == null || data[0] == null) {
            errorCollector.addError("Invalid input for user data: " + data.toString());
            return;
        }
        try {
            this.id = Long.parseLong(data[0]);
        } catch (NumberFormatException e) {
            errorCollector.addError("Invalid input for id: " + data[0]);
            return;
        }

        try {
            this.name = data[1];
        } catch (Exception e) {
            errorCollector.addError("Invalid input for name: " + data[1] + " (id: " + data[0] + ")");
        }

        try {
            if (data[2].equals("男")) {
                this.sex = 'M';
            } else if (data[2].equals("女")) {
                this.sex = 'F';
            } else {
                this.sex = 'X';
            }
        } catch (Exception e) {
            errorCollector.addError("Invalid input for sex: " + data[2] + " (id: " + data[0] + ")");
        }

        try {
            Pattern p = Pattern.compile("(\\d+)");
            Matcher m = p.matcher(data[3]);
            if (m.find()) {
                this.birthday = Integer.parseInt(m.group(0));
                this.birthmonth = Integer.parseInt(m.group(1));
            } else {
                this.birthday = 0;
                this.birthmonth = 0;
            }
        } catch (NumberFormatException e) {
            errorCollector.addError("Invalid input for birthday or birthmonth: " + data[3] + " (id: " + data[0] + ")");
        }

        try {
            this.level = Integer.parseInt(data[4]);
        } catch (NumberFormatException e) {
            errorCollector.addError("Invalid input for level: " + data[4] + " (id: " + data[0] + ")");
        }

        try {
            this.sign = data[5];
        } catch (Exception e) {
            errorCollector.addError("Invalid input for sign: " + data[5] + " (id: " + data[0] + ")");
        }

        try {
            String followingStr = data[6].substring(1, data[6].length() - 1);
            String[] followingArr = followingStr.split(",\\s*");
            if (followingArr.length == 1 && followingArr[0].equals("")) {
                this.following = new long[0];
                return;
            }
            this.following = new long[followingArr.length];
            for (int i = 0; i < followingArr.length; i++) {
                this.following[i] = Long.parseLong(followingArr[i].replace("'", "").trim());
            }
        } catch (NumberFormatException e) {
            errorCollector.addError("Invalid input for following: " + data[6] + " (id: " + data[0] + ")");
        }

        try {
            if(data.length > 7)
                this.identity = data[7];
            else{
                this.identity = "";
                errorCollector.addError("Invalid input for identity: no input" + " (id: " + data[0] + ")");
            }
        } catch (Exception e) {
            errorCollector.addError("Invalid input for identity: " + data[7] + " (id: " + data[0] + ")");
        }
    }

    public static boolean check(User user){
        //to check if the data is valid
        if(user.id == null){
            System.out.println("Invalid id for user: " + user.toString());
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex=" + sex +
                ", birthday=" + birthday +
                ", birthmonth=" + birthmonth +
                ", level=" + level +
                ", sign='" + sign + '\'' +
                ", following=" + Arrays.toString(following) +
                ", identity='" + identity + '\'' +
                '}';
    }
    
}
