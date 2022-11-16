import pandas as pd

# xl = pd.ExcelFile("university_data.xlsx")

# for sheet in xl.sheet_names:
#     print("----------",sheet,"--------")
#     dataframe = xl.parse(sheet).to_dict(orient="record")
#     for d in dataframe[:50]:
#         print(tuple(d.values()))



import configparser
import snowflake.connector
import os

cfg = configparser.ConfigParser()
cfg.read('config.cfg')

conn = snowflake.connector.connect(
        user=cfg.get('Snowflake','user'),
        password=cfg.get('Snowflake','password'),
        account=cfg.get('Snowflake','account'),
        warehouse=cfg.get('Snowflake','warehouse'),
        database=cfg.get('Snowflake','database'),
        schema=cfg.get('Snowflake','schema'))

cur=conn.cursor()
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
            elif s[0]=='Humanities':
                percent=((s[1])/400)*100
            
            def Update_percentage(percent):
                cur.execute(f'update student set percentage={percent} where student_id={s[2]}')

                if percent>=90:
                    cur.execute(f'update student set scholarship_eligibility=True where student_id={s[2]}')
            

            Update_percentage(percent)
        print('all percent updated')    


    # percentage(data)

def creat_text_file():
    text_writer = open("output.txt", "w+")
   

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
    text_writer.write('\n\n\n\n')

    
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
    text_writer.write('\n\n\n\n')

    #3       
    cur.execute('''
    select name,stream,college,total_marks, percentage from(
    select s.name as name, s.total_marks, s.percentage, c.name as college,s.stream,
    dense_rank() over (PARTITION  by s.stream order by s.total_marks desc) as rnk
    from student s,college c where  c.college_id=s.college_id
    )where rnk=1;
    ''')
    
    steram_topper = cur.fetch_pandas_all().set_index("NAME")
    print(steram_topper)
    text_writer.write("3. Stream Toppers.\n\n")
    text_writer.write(str(steram_topper))
    text_writer.write('\n\n\n\n')


#     #6
    cur.execute('''
        select s.name, s.stream, c.name as college_name,s.scholarship_eligibility
        from student s
        inner join college c on   c.college_id=s.college_id
        where s.scholarship_eligibility=true;
        ''')
    scholarship = cur.fetch_pandas_all().set_index("NAME")
    text_writer.write("Que 4. Given bellow students got scholarship.\n\n")
    text_writer.write(str(scholarship))
    text_writer.write('\n\n\n\n')


    text_writer.close()

    print(open("output.txt", "r").read())



creat_text_file()

