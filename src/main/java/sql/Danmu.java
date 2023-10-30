package sql;

public class Danmu {
    String video_id;
    long user_id;
    float time;
    String content;
    long id;
    static long count = 0;

    public Danmu(String[] data, ErrorCollector errorCollector){
        if(data == null){
            errorCollector.addError("Invalid input for danmu data: " + data);
            return;
        }
        try{
            this.video_id = data[0];
        }catch (Exception e){
            errorCollector.addError("Invalid input for video_id: " + data[0]);
            return;
        }

        try{
            this.user_id = Long.parseLong(data[1]);
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for user_id: " + data[1] + " (video_id: " + data[0] + ")");
        }

        try{
            this.time = Float.parseFloat(data[2]);
        }catch (NumberFormatException e){
            errorCollector.addError("Invalid input for time: " + data[2] + " (video_id: " + data[0] + ")");
        }

        try{
            this.content = data[3];
        }catch (Exception e){
            errorCollector.addError("Invalid input for content: " + data[3] + " (video_id: " + data[0] + ")");
        }

        count++;

        this.id = count;
    }
}
