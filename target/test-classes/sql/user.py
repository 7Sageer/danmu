import pandas as pd
import psycopg2
from tqdm import tqdm
import ast
# 重新打开数据库连接
params = {
    "host": "localhost",
    "database": "danmu",
    "user": "postgres",
    "password": "747576"
}
conn = psycopg2.connect(**params)
cur = conn.cursor()

df = pd.read_csv('data/users.csv',encoding='utf-8')



# 遍历每行数据
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    if isinstance(row['Birthday'], str):
        try:
            birth_parts = row['Birthday'].split('月')
            birth_month = int(birth_parts[0])
            birth_day = int(birth_parts[1].replace('日', ''))
        except:
            continue
    else:
        continue
    
    # 处理性别
    if row['Sex'] == '男':
        sex = 'M'
    elif row['Sex'] == '女':
        sex = 'F'
    else:
        sex = 'X'  # 如果不是男或女，默认为X

    # 插入到user_info表
    cur.execute("""
    INSERT INTO user_info (user_id, name, sex, BirthMonth, BirthDay, sign)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
    """, (row['Mid'], row['Name'], sex, birth_month, birth_day, row['Sign']))

    # 插入到user_role表
    cur.execute("""
    INSERT INTO user_role (user_id, level, role)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
    """, (row['Mid'], row['Level'], row['identity']))

    # 处理following字段
    following_ids = ast.literal_eval(row['following'])
    for following_id in following_ids:
        cur.execute("""
        INSERT INTO user_following (FollowerUserId, FollowingUserId)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """, (row['Mid'], following_id))

# 提交事务
conn.commit()

# 关闭游标和连接
cur.close()
conn.close()