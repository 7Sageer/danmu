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

public class DanmuUploader {

    public static void uploadDanmus(ErrorCollector errorCollector, int batchNum, CSVReader dr) throws IOException {
        try {
            int threadNum = 20;
            ProgressBar progressBar;
            long total;
            total = Files.lines(Paths.get("data\\danmu1.csv")).count();
            progressBar = new ProgressBar(total);
            AtomicBoolean isEnd = new AtomicBoolean(false);
            BlockingQueue<ArrayList<Danmu>> queue = new LinkedBlockingQueue<>();
            new Thread(() -> {
                try {
                    while (dr.peek() != null) {
                        queue.put(Reader.readDanmus(batchNum, errorCollector, dr));
                        while (queue.size() > 10) {
                            Thread.sleep(100);
                            // System.out.print("Waiting for updating");
                        }
                    }
                    isEnd.set(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            ExecutorService executor = Executors.newFixedThreadPool(threadNum);
            Runnable task = () -> {
                try (Connection threadCon = DatabaseConnectionPool.getConnection()) {
                    ArrayList<Danmu> danmus = queue.take();
                    threadCon.setAutoCommit(false);
                    PreparedStatement danmu_info = threadCon
                            .prepareStatement("insert into danmu_info(danmu_id, video_id, user_id, time, content)"
                                    + "values(?,?,?,?,?)");
                    for (Danmu i : danmus) {
                        uploadDanmu(i, errorCollector, danmu_info);
                    }
                    System.out.printf("%d lines read.", progressBar.getcurrent());
                    synchronized (progressBar) {
                        progressBar.update(batchNum);
                    }
                    threadCon.commit();
                    threadCon.close();
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
            System.out.println("Start uploading danmus");

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

            progressBar.end();

            System.out.printf("%d lines read.", progressBar.getcurrent());
            progressBar.end();
            dr.close();
            errorCollector.displayErrors();
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void uploadDanmu(Danmu danmu, ErrorCollector errorCollector, PreparedStatement danmu_info) {
        try {
            if (danmu.id == 0) {
                return;
            }
            danmu_info.setLong(1, danmu.id);
            danmu_info.setLong(3, danmu.user_id);
            danmu_info.setString(2, danmu.video_id);
            danmu_info.setFloat(4, danmu.time);
            danmu_info.setString(5, danmu.content);
            danmu_info.addBatch();
        } catch (SQLException e) {
            errorCollector.addError(e.toString());
        }
    }
}
