package sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.opencsv.CSVReader;

public class VideoUploader {
	public static void uploadVideos(ErrorCollector errorCollector, CSVReader vr, int threadNum, int BATCHNUM)
			throws IOException {
		BlockingQueue<ArrayList<Video>> queue = new LinkedBlockingQueue<>();
		// total = Files.lines(Paths.get("data\\videos.csv")).count();
		int total = 7865;
		System.out.println(total);
		ProgressBar progressBar = new ProgressBar(total);
		AtomicBoolean isEnd = new AtomicBoolean(false);
		new Thread(() -> {
			try {
				while (vr.peek() != null) {
					queue.put(Reader.readVideos(BATCHNUM, errorCollector, vr));
					while (queue.size() > 10) {
						Thread.sleep(100);
						System.out.print("Waiting for updating");
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
				ArrayList<Video> videos = queue.take();
				threadCon.setAutoCommit(false);
				PreparedStatement video_info = threadCon
						.prepareStatement("insert into video_info(video_id, title, description,duration)"
								+ "values(?,?,?,?)");
				PreparedStatement video_action = threadCon
						.prepareStatement("insert into video_action(video_id, user_id, action)"
								+ "values(?,?,?)");
				PreparedStatement video_status = threadCon.prepareStatement(
						"insert into video_status(video_id, owner_id, create_time, review_time,public_time, reviewer_id)"
								+ "values(?,?,?,?,?,?)");
				PreparedStatement video_view = threadCon
						.prepareStatement("insert into video_view(video_id, user_id, view_duration)"
								+ "values(?,?,?)");
				for (Video i : videos) {
					uploadVideo(i, errorCollector, video_info, video_action, video_status,
							video_view);
				}
				video_info.executeBatch();
				video_status.executeBatch();
				// video_view 和 video_action 是一对多的关系，在upploadVideo中已经执行了executeBatch
				System.out.printf("%d lines read.", progressBar.getcurrent());
				synchronized (progressBar) {
					progressBar.update(BATCHNUM);
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
		System.out.println("Start uploading videos");

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
		vr.close();
		errorCollector.displayErrors();
	}

	private static void uploadVideo(Video video, ErrorCollector errorCollector, PreparedStatement video_info,
			PreparedStatement video_action, PreparedStatement video_status, PreparedStatement video_view) {
		if (!Video.check(video)) {
			return;
		}
		try {
			video_info.setString(1, video.id);
			video_info.setString(2, video.title);
			video_info.setString(3, video.description);
			video_info.setInt(4, video.duration);
			video_info.addBatch();
			video_status.setString(1, video.id);
			video_status.setLong(2, video.author_id);

			if (video.commit_time != null) {
				video_status.setTimestamp(3, Timestamp.valueOf(video.commit_time));
			} else {
				video_status.setNull(3, Types.TIMESTAMP);
			}
			if (video.review_time != null) {
				video_status.setTimestamp(4, Timestamp.valueOf(video.review_time));
			} else {
				video_status.setNull(4, Types.TIMESTAMP);
			}
			if (video.public_time != null) {
				video_status.setTimestamp(5, Timestamp.valueOf(video.public_time));
			} else {
				video_status.setNull(5, Types.TIMESTAMP);
			}

			video_status.setLong(6, video.reviewer_id);
			video_status.addBatch();
			if (video.view != null) {
				int count = 0;
				for (Entry<Long, Integer> i : video.view) {
					try {
						// System.out.println(video.id);
						video_view.setString(1, video.id);
						video_view.setLong(2, i.getKey());
						video_view.setInt(3, i.getValue());
						video_view.addBatch();
					} catch (Exception e) {
						System.out.println(video.id);
						System.out.println(i.getKey());
						System.out.println(i.getValue());
					}
					count++;
				}
				video_view.executeBatch();
			}
			if (video.like != null) {
				int count = 0;
				for (Long i : video.like) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "like");
					video_action.addBatch();
					count++;
				}
				video_action.executeBatch();
			} else {
				// System.out.println("Like list is null");
			}
			if (video.coin != null) {
				int count = 0;
				for (Long i : video.coin) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "coin");
					video_action.addBatch();
					count++;
				}
				video_action.executeBatch();
			} else {
				// System.out.println("Coin list is null");
			}
			if (video.favorite != null) {
				int count = 0;
				for (Long i : video.favorite) {
					video_action.setString(1, video.id);
					video_action.setLong(2, i);
					video_action.setString(3, "favorite");
					video_action.addBatch();
					count++;
				}
				video_action.executeBatch();
			} else {
				// System.out.println("Favorite list is null");
			}
		} catch (SQLException e) {
			errorCollector.addError(e.toString());
			e.printStackTrace();
			errorCollector.displayErrors();
			// System.exit(1);
		}
	}
}
