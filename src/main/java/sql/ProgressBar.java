package sql;

public class ProgressBar {
    private long total;
    private long current;
    private long startTime;
    private final static int BARLENGTH = 50;

    public ProgressBar(long total) {
        this.total = total;
        this.current = 0;
        this.startTime = System.currentTimeMillis();
    }


    public void update(long increment) {
        this.current += increment;
        long elapsedTime = System.currentTimeMillis() - startTime;
        double progress = (double) current / total;
        
        // 计算ETA
        double avgTimePerUnit = (double) elapsedTime / current;
        long etaMillis = (long) (avgTimePerUnit * (total - current));
        long etaSeconds = etaMillis / 1000;

        // 打印进度条和ETA
        System.out.print("Progress: [");
        int position = (int) (BARLENGTH * progress);
        for (int i = 0; i < BARLENGTH; i++) {
            if (i < position) System.out.print("=");
            else if (i == position) System.out.print(">");
            else System.out.print(" ");
        }
        System.out.print("] " + Math.round(progress * 100) + "% ETA: " + etaSeconds + "s\r");
    }

    public void end() {
        current = total;
        System.out.print("Progress: [");
        for (int i = 0; i < BARLENGTH; i++) {
            System.out.print("=");
        }
        System.out.print("] 100% ETA: 0s");
        System.out.println();
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Total time elapsed: " + elapsedTime + "ms");
    }

    public long getcurrent(){
        return this.current;
    }
}

