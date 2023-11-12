truncate danmu;
copy danmu(video_id, user_id, time, content) FROM 'D:\2023FALL\sql\project_1\data\danmu.csv' WITH (FORMAT csv, HEADER true);
