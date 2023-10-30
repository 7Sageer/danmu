package sql;

import java.util.ArrayList;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reader {
    private static int errorCount = 0;

    public static ArrayList<User> readUsers(int n, ErrorCollector errorCollector, CSVReader reader) {
        ArrayList<User> users = new ArrayList<>();
        try {
            String[] nextLine;
            if (n != 0) {
                for (int i = 0; i < n && (nextLine = reader.readNext()) != null; i++) {
                    User user = new User(nextLine, errorCollector);
                    users.add(user);
                }
            } else {
                while ((nextLine = reader.readNext()) != null) {
                    User user = new User(nextLine, errorCollector);
                    users.add(user);
                }
            }
        } catch (CsvValidationException e) {
            errorCollector.addError(e.toString());
        } catch (IOException e) {
            errorCollector.addError(e.toString());
        }
        // errorCollector.displayErrors();
        return users;
    }

    public static ArrayList<Video> readVideos(int n, ErrorCollector errorCollector, CSVReader reader) {
        ArrayList<Video> videos = new ArrayList<>();
        try {
            String[] nextLine = null;
            if (n != 0) {
                for (int i = 0; i < n; i++) {
                    try {
                        nextLine = reader.readNext();
                        if (nextLine != null) {
                            Video video = new Video(nextLine, errorCollector);
                            videos.add(video);
                        }
                    } catch (CsvMalformedLineException e) {
                        // 记录或输出异常信息
                        errorCollector.addError("Malformed line: " + Arrays.toString(nextLine));
                        System.exit(nextLine.length);

                    }
                }
            } else {
                while (true) {
                    try {
                        nextLine = reader.readNext();
                        if (nextLine == null) {
                            break;
                        }
                        Video video = new Video(nextLine, errorCollector);
                        videos.add(video);
                    } catch (CsvMalformedLineException e) {
                        // 记录或输出异常信息
                        errorCollector.addError("Malformed line: " + Arrays.toString(nextLine));
                        System.exit(nextLine.length);
                    }
                }
            }

        } catch (CsvValidationException e) {
            errorCollector.addError(e.toString());
        } catch (IOException e) {
            errorCollector.addError(e.toString());
        }
        // errorCollector.displayErrors();
        return videos;
    }

    public static ArrayList<Danmu> readDanmus(int n, ErrorCollector errorCollector, CSVReader reader) {
        ArrayList<Danmu> danmus = new ArrayList<>();
        try {
            String[] nextLine;
            if (n != 0) {
                for (int i = 0; i < n && (nextLine = reader.readNext()) != null; i++) {
                    if(nextLine.length != 4){

                        errorCollector.addError("Invalid input for danmu data: " + Arrays.toString(nextLine));
                        System.out.println();
                        if(nextLine.length >= 4)
                            System.out.println("Invalid input for danmu data: "  + '\n' + nextLine[3].substring(0,150));
                        errorCount++;
                        continue;

                    }
                    Danmu danmu = new Danmu(nextLine, errorCollector);
                    danmus.add(danmu);
                }
            } else {
                while ((nextLine = reader.readNext()) != null) {
                    Danmu danmu = new Danmu(nextLine, errorCollector);
                    danmus.add(danmu);
                }
            }
        } catch (CsvValidationException e) {
            errorCollector.addError(e.toString());
        } catch (IOException e) {
            errorCollector.addError(e.toString());
        }
        // errorCollector.displayErrors();
        return danmus;
    }

    public static void main(String[] args) throws CsvValidationException, IOException {

        ArrayList<Video> videos = new ArrayList<>();
        // ArrayList<Danmu> danmus = new ArrayList<>();
        ErrorCollector errorCollector = new ErrorCollector();
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream("data\\videos.csv"), "UTF-8"));
        int count = 0;
        while (reader.readNext() != null) {
            videos = readVideos(10, errorCollector, reader);
            count++;
            if(count%10==0){
                System.out.printf("%d videos data loaded.\n", videos.size());
            }
        }

        // try (CSVReader reader = new CSVReader(new FileReader("data\\danmu.csv"))) {
        // String[] header = reader.readNext();
        // String[] nextLine;
        // int count = 1;

        // while ((nextLine = reader.readNext()) != null) {
        // nextLine = reader.readNext();
        // count++;
        // Danmu danmu = new Danmu(nextLine, errorCollector);
        // // danmus.add(danmu);
        // }

        // System.out.printf("%d lines read.\n", count - 1);
        // errorCollector.displayErrors();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }

        System.out.printf("%d videos data loaded.\n", videos.size());
        // System.out.printf("%d danmus data loaded.\n", danmus.size());
    }
}
