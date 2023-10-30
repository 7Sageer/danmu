package sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import com.opencsv.CSVReader;

public class DanmuUploader {
    
    public static void uploadDanmus(ErrorCollector errorCollector, int batchNum, CSVReader dr) throws IOException {
        try {
            ProgressBar progressBar;
            long total;
            Connection con = DatabaseConnectionPool.getConnection();
            con.setAutoCommit(false);
            total = Files.lines(Paths.get("data\\danmu1.csv")).count();
            progressBar = new ProgressBar(total);
            PreparedStatement danmu_info = con
                    .prepareStatement("insert into danmu(id,video_id,user_id, time, content)"
                            + "values(?,?,?,?,?)");
            while (dr.peek() != null) {
                ArrayList<Danmu> danmus = Reader.readDanmus(batchNum, errorCollector, dr);

                for (Danmu i : danmus) {
                    uploadDanmu(i, errorCollector, danmu_info);
                }
                danmu_info.executeBatch();
                System.out.printf("%d lines read.", progressBar.getcurrent());
                progressBar.update(batchNum);
            }
            progressBar.end();
            dr.close();
            con.commit();
            errorCollector.displayErrors();
        } catch (SQLException e) {
            System.out.println(e.toString());
        }
    }

    private static void uploadDanmu(Danmu danmu, ErrorCollector errorCollector, PreparedStatement danmu_info) {
        try {
            if(danmu.id == 0){
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
