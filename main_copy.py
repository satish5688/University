import configparser
import snowflake.connector
import sys, getopt
import json
import os
import pandas as pd



def initilize(path):
    print("Initilizing...")
    cfg = configparser.ConfigParser()
    cfg.read(r"{}".format(path))


    conn = snowflake.connector.connect(
        user=cfg.get('Snowflake','user'),
        password=cfg.get('Snowflake','password'),
        account=cfg.get('Snowflake','account'),
        warehouse=cfg.get('Snowflake','warehouse'),
        database=cfg.get('Snowflake','database'),
        schema=cfg.get('Snowflake','schema'))

    print("Initilized...")
    return conn.cursor()

def create(folder_path):
    entries = os.listdir(folder_path)
    for s in entries:
        with open ("{}/{}".format(folder_path, s),"r") as  stu:
            data=json.load(stu)
            format=", ".join(list(map(lambda x: "{} {}".format( x,data[x]),data) ))
            cur.execute(
            "CREATE OR REPLACE  TABLE "
            f"{s[:-5]}({format})")
            print(f'{s[:-5]} table successfully created')


def insert():
    xl = pd.ExcelFile(folder_path)
    for sheet in xl.sheet_names:
        print("----------",sheet,"--------")
        dataframe = xl.parse(sheet).to_dict(orient="record")
        for d in dataframe:
            cur.execute(
                f"INSERT INTO {sheet} VALUES {(tuple(d.values()))}".replace("nan", "null"))


def output():
    sub_data=cur.execute('select c.type,s.total_marks,  s.student_id from student s, college c where c.college_id=s.college_id;')
    data=sub_data.fetchall()
    def percentage(data):
        for s in data:
            if s[0]=="Medical":
                percent=((s[1])/500)*100
            elif s[0]=='Engineering':
                percent=((s[1])/1000)*100
            elif s[0]=='Science':
                percent=((s[1])/700)*100
            elif s[0]=='Humanities':
                percent=((s[1])/400)*100
            
            def update_percentage(percent):
                cur.execute(f'update student set percentage={percent} where student_id={s[2]}')

                if percent>=90:
                    cur.execute(f'update student set scholarship_eligibility=True where student_id={s[2]}')
            

            update_percentage(percent)
        print('all percent updated')    


    percentage(data)

def creat_text_file(folder_path):

    text_writer = open(folder_path, "w+")
   

    #1
    cur.execute('''
    select name,stream,college_name,percentage from
    (select  s.name,s.stream,c.name as college_name,s.percentage,
    rank() over(order by percentage desc)as rnk
    from student s
    inner join college c
    where c.college_id=s.college_id )where rnk=1;
     ''')
    
    university_topper = cur.fetch_pandas_all().set_index("NAME")
    text_writer.write("1. Univercity Topper.\n\n")
    text_writer.write(str(university_topper))
    text_writer.write('\n\n------END------\n\n\n\n')

    
    #2
    cur.execute('''
        select  college,name,total_marks, percentage from(
        select s.name as name, s.total_marks, s.percentage, c.name as college,
        rank() over (PARTITION  by s.college_id order by s.total_marks desc) as rnk
        from student s,college c where  c.college_id=s.college_id
        )where rnk=1;
        ''')
    college_toppers = cur.fetch_pandas_all().set_index("COLLEGE")
    text_writer.write("2. College_Toppers.\n\n")
    text_writer.write(str(college_toppers))
    text_writer.write('\n\n------END------\n\n\n\n')

    #3       
    cur.execute('''
    select name,stream,college,total_marks, percentage from(
    select s.name as name, s.total_marks, s.percentage, c.name as college,s.stream,
    dense_rank() over (PARTITION  by s.stream order by s.total_marks desc) as rnk
    from student s,college c where  c.college_id=s.college_id
    )where rnk=1;
    ''')
    
    steram_topper = cur.fetch_pandas_all().set_index("NAME")
    text_writer.write("3. Stream Toppers.\n\n")
    text_writer.write(str(steram_topper))
    text_writer.write('\n\n--------END------\n\n\n\n')


    #6
    cur.execute('''
        select s.name, s.stream, c.name as college_name,s.scholarship_eligibility
        from student s
        inner join college c on   c.college_id=s.college_id
        where s.scholarship_eligibility=true;
        ''')
    scholarship = cur.fetch_pandas_all().set_index("NAME")
    text_writer.write("4. Given bellow students got scholarship.\n\n")
    text_writer.write(str(scholarship))
    text_writer.write('\n\n------END------\n\n\n\n')

    text_writer.close()
    print(open(folder_path).read())




            


if __name__ == "__main__":
    args = sys.argv[1:]
    opts = getopt.getopt(args, "t:c:d:")[0]
    function_type = ""
    config_path = ""
    folder_path = ""
    for o in opts:
        if o[0] == "-t":
            function_type = o[1]
        elif o[0] == "-c":
            config_path = o[1]
        elif o[0] == "-d":
            folder_path = o[1]
    if not (function_type and config_path and folder_path):
        print("Invalid paremeters...")

    cur = initilize(config_path)
    if function_type == "create":
        create(folder_path)
    elif function_type == "insert":
        insert()
    elif function_type== 'fetch':
        output()
        print('about to work')
        creat_text_file(folder_path)









# create
# python main_copy.py -t create -c \Users\satish.mungusmare\Desktop\Projects\University\config.cfg -d query_dataset    

# Insert
#  python main_copy.py -t insert -c \Users\satish.mungusmare\Desktop\Projects\University\config.cfg -d university_data.xlsx

# fetch
# python main_copy.py -t fetch -c \Users\satish.mungusmare\Desktop\Projects\University\config.cfg -d output.txt     