package sql;

import java.util.concurrent.atomic.AtomicLong;

public class ProgressBar {
    private long total;
    private AtomicLong current;
    private long startTime;
    private long lastTime;
    private long lastCurrent;
    private final static int BARLENGTH = 50;

    public ProgressBar(long total) {
        this.total = total;
        this.current = new AtomicLong(0);
        this.startTime = System.currentTimeMillis();
        this.lastTime = startTime;
        this.lastCurrent = 0;
    }

    public synchronized void update(long increment) {
        this.current.addAndGet(increment);
        long now = System.currentTimeMillis();
        double progress = (double) current.get() / total;

        // 计算速度
        long timeDiff = now - lastTime;
        long currentDiff = current.get() - lastCurrent;
        double speed = (double) currentDiff / (timeDiff / 1000.0);

        // 计算ETA
        double avgTimePerUnit = (double) (now - startTime) / current.get();
        long etaMillis = (long) (avgTimePerUnit * (total - current.get()));
        long etaSeconds = etaMillis / 1000;

        // 清除当前行并打印进度条
        System.out.print("\r"); // 清除当前行
        System.out.print("Progress: [");
        int position = (int) (BARLENGTH * progress);
        for (int i = 0; i < BARLENGTH; i++) {
            if (i < position) System.out.print("=");
            else if (i == position) System.out.print(">");
            else System.out.print(" ");
        }
        System.out.print("] " + Math.round(progress * 100) + "% Speed: " + String.format("%.2f", speed) + "units/s ETA: " + etaSeconds + "s");

        lastTime = now;
        lastCurrent = current.get();
    }

    public void end() {
        current.set(total);
        update(0);
        System.out.println("\nTotal time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
    }

    public long getcurrent(){
        return this.current.get();
    }
}
