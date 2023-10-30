package sql;
import java.sql.Time;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.*;

public class Video {
    String id;
    String title;
    long author_id;
    String author_name;
    LocalDateTime commit_time;
    LocalDateTime review_time;
    LocalDateTime public_time;
    int duration;
    String description;
    long reviewer_id;
    long [] like;
    long [] coin;
    long [] favorite;
    List<Map.Entry<Long, Integer>> view;

    public static boolean check(Video video){
        //to check if the data is valid
        if(video == null){
            return false;
        }
        if(video.id == null || video.id.equals("")){
            return false;
        }
        return true;
    }
    public Video(String id){
        this.id = id;
    }

    public Video(String data[],ErrorCollector errorCollector){
        if(data == null||data.length != 14){
            if(data == null){
                errorCollector.addError("Invalid input for video data: null");
            }
            else{
                System.out.println(data.length);
            }
            return;
        }
        try{
            this.id = data[0];
        }catch (Exception e){
            errorCollector.addError("Invalid input for id: " + data[0]);
            return;
        }

        try{
            this.title = data[1];
        }catch (Exception e){
            errorCollector.addError("Invalid input for title: " + data[1] + " (id: " + data[0] + ")");
        }

        try{
            this.author_id = Long.parseLong(data[2]);
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for author_id: " + data[2] + " (id: " + data[0] + ")");
        }

        try{
            this.author_name = data[3];
        }catch (Exception e){
            errorCollector.addError("Invalid input for author_name: " + data[3] + " (id: " + data[0] + ")");
        }

        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            this.commit_time = LocalDateTime.parse(data[4],formatter);
        }catch (Exception e){
            errorCollector.addError("Invalid input for commit_time: " + data[4] + " (id: " + data[0] + ")");
        }

        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            this.review_time = LocalDateTime.parse(data[5],formatter);
        }catch (Exception e){
            errorCollector.addError("Invalid input for review_time: " + data[5] + " (id: " + data[0] + ")");
        }

        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            this.public_time = LocalDateTime.parse(data[6],formatter);
        }catch (Exception e){
            errorCollector.addError("Invalid input for public_time: " + data[6] + " (id: " + data[0] + ")");
        }

        try{
            this.duration = Integer.parseInt(data[7]);
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for duration: " + data[7] + " (id: " + data[0] + ")");
        } 

        try{
            this.description = data[8];
        }catch (Exception e){
            errorCollector.addError("Invalid input for description: " + data[8] + " (id: " + data[0] + ")");
        }

        try{
            this.reviewer_id = Long.parseLong(data[9]);
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for reviewer_id: " + data[9] + " (id: " + data[0] + ")");
        }

        try{
            String likeStr = data[10].substring(1, data[10].length() - 1);
            String[] likeStrs = likeStr.split(",\\s*");            
            if (likeStrs.length == 1 && likeStrs[0].equals("")) {
                this.like = new long[0];
                return;
            }
            this.like = new long[likeStrs.length];
            for(int i = 0;i < likeStrs.length;i++){
                this.like[i] = Long.parseLong(likeStrs[i].replace("'", "").trim());
            }
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for like: " + data[10] + " (id: " + data[0] + ")");
        }

        try{
            String coinStr = data[11].substring(1, data[11].length() - 1);
            String[] coinStrs = coinStr.split(",\\s*");            
            if (coinStrs.length == 1 && coinStrs[0].equals("")) {
                this.coin = new long[0];
                return;
            }
            this.coin = new long[coinStrs.length];
            for(int i = 0;i < coinStrs.length;i++){
                this.coin[i] = Long.parseLong(coinStrs[i].replace("'", "").trim());
            }
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for coin: " + data[11] + " (id: " + data[0] + ")");
        }

        try{
            String favoriteStr = data[12].substring(1, data[12].length() - 1);
            String[] favoriteStrs = favoriteStr.split(",\\s*");            
            if (favoriteStrs.length == 1 && favoriteStrs[0].equals("")) {
                this.favorite = new long[0];
                return;
            }
            this.favorite = new long[favoriteStrs.length];
            for(int i = 0;i < favoriteStrs.length;i++){
                this.favorite[i] = Long.parseLong(favoriteStrs[i].replace("'", "").trim());
            }
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for favorite: " + data[12] + " (id: " + data[0] + ")");
        }

        try{
                String viewStr = data[13];
                
                List<Map.Entry<Long, Integer>> viewList = new ArrayList<>();
                Pattern pattern = Pattern.compile("\\('(\\d+)',\\s*(\\d+)\\)");
                Matcher matcher = pattern.matcher(viewStr);
                
                while (matcher.find()) {
                    Long userId = Long.parseLong(matcher.group(1));
                    Integer duration = Integer.parseInt(matcher.group(2));
                    viewList.add(new AbstractMap.SimpleEntry<>(userId, duration));
                }
                
                this.view = viewList;
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for view: " + data[13] + " (id: " + data[0] + ")");
        }
    }

}
