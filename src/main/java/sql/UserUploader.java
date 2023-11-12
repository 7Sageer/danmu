package sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.opencsv.CSVReader;

public class UserUploader {

    public static void uploadUsers(ErrorCollector errorCollector, int batchNum, CSVReader ur) throws IOException {
        try {
            ProgressBar progressBar;
            int threadNum = 20;
            long total;
            total = Files.lines(Paths.get("data\\users.csv")).count();
            progressBar = new ProgressBar(total);
            AtomicBoolean isEnd = new AtomicBoolean(false);
            BlockingQueue<ArrayList<User>> queue = new LinkedBlockingQueue<>();
            new Thread(() -> {
                try {
                    while (ur.peek() != null) {
                        queue.put(Reader.readUsers(batchNum, errorCollector, ur));
                    }
                    isEnd.set(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            ExecutorService executor = Executors.newFixedThreadPool(threadNum);
            Runnable task = () -> {
                try (Connection threadCon = DatabaseConnectionPool.getConnection()) {
                    ArrayList<User> users = queue.take();
                    threadCon.setAutoCommit(false);
                    PreparedStatement user_info = threadCon
                            .prepareStatement("insert into user_info(user_id, name, sex, birthmonth, birthday, sign)"
                                    + "values(?,?,?,?,?,?)");
                    PreparedStatement user_role = threadCon
                            .prepareStatement("insert into user_role(user_id, level, role)"
                                    + "values(?,?,?)");
                    PreparedStatement user_following = threadCon
                            .prepareStatement("insert into user_following(followeruserid, followinguserid)"
                                    + "values(?,?)");
                    for (User i : users) {
                        uploadUser(i, errorCollector, user_info, user_role, user_following);
                        user_following.executeBatch();
                        user_info.executeBatch();
                        user_role.executeBatch();
                    }
                    threadCon.commit();
                    System.out.printf("%d lines read.", progressBar.getcurrent());
                    synchronized (progressBar) {
                        progressBar.update(batchNum);
                    }
                    
                } catch (SQLException e) {
                    System.out.println(e.toString());
                    e.printStackTrace();
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            };
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threadNum; i++) {
                futures.add(executor.submit(task));
            }
            System.out.println("Start uploading users");

            while (true) {
                Iterator<Future<?>> iterator = futures.iterator();
                List<Future<?>> newFutures = new ArrayList<>();

                while (iterator.hasNext()) {
                    Future<?> future = iterator.next();
                    if (future.isDone() || future.isCancelled()) {
                        iterator.remove(); // 删除当前元素
                        newFutures.add(executor.submit(task)); // 向临时列表添加新元素
                    }
                }

                futures.addAll(newFutures); // 把新任务添加到主列表

                if (queue.peek() == null && isEnd.get()) {
                    executor.shutdown();
                    break;
                }
            }


            System.out.printf("%d lines read.", progressBar.getcurrent());
            
            ur.close();
            //errorCollector.displayErrors();
            progressBar.end();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
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
