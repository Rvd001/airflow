import json
import os
from datetime import datetime, timedelta
from jinja2.environment import Template
import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.email_operator import email_operator
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.operators.python_operator import PythonOperator
from jinja2 import Environment, FileSystemLoader

S3_FILE_NAME = f"{datetime.today().date()}_top_questions.json"


#--------------------DAG--------------------------

default_args = {

    "owner": "me",
    "depends_on_past":False,
    "start_date": datetime(2019,1,9),
    "email": ["my_email@mail.com"],
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes = 1),
    "schedule_interval":"@daily",
}


with DAG("stack_overflow_questions", default_args = default_args) as dag:
    
    #create a table in postgres
    #Postgres Connections
    #Postgres Operator

    Task_I = PostgresOperator(
        task_id = "create_table",
        database = "stack_overflow",
        postgres_conn_id = "postgres_connection",
        sql = """
        DROP TABLE IF EXISTS public.questions;
        CREATE TABLE public.questions
        (
            title text,
            is_answered boolean,
            link character varying
            score integer,
            tags text[],
            question_id integer NOT NULL,
            owner_reputation integer
        )
        """
    )


    #Store data from Stack Overflow into the created Table
    #Postgres Connection
    #Variable
    #PythonOperators
    Task_II = PythonOperator(
        task_id = "insert_questions",
        python_callable = insert_question_to_db
    )


    Task_III = PythonOperator(
        task_id = "write_questions_to_s3",
        python_callable = write_questions_to_s3
    )


    Task_IV = PythonOperator(
        task_id = "render_template",
        python_callable = render_template,
        provide_content = True
    )



    Task_IV = PythonOperator(

        
    )    


    def call_stack_overflow_api() -> dict:
        """Get First 100 Questions created two days ago sorted by user votes"""

        stack_overflow_question_url = Variable.get("STACK_OVERFLOW_QUESTION_URL")

        today = datetime.now()
        two_days_ago = today - timedelta(days = 2)
        three_days_ago = today - timedelta(days = 3)

        payload = {
            "fromdate": int(datetime.timestamp(three_days_ago)),
            "todate": int(datetime.timestamp(two_days_ago)),
            "sort": "votes",
            "site": "stackoverflow",
            "order": "desc",
            "tagged": Variable.get("TAG"),
            "client_id": Variable.get("STACK_OVERFLOW_CLIENT_ID"),
            "Client_secret": Variable.get("STACK_OVERFLOW_CLIENT_SECRET"),
            "Key": Variable.get("STCK_OVERFLOW_KEY")
        }

        response = requests.get(stack_overflow_question_url, params = payload)

        for question in response.json().get("items", []):
            yield {
                "question_id": question["question_id"],
                "title" : question["title"],
                "is_answered" : question["is_answered"],
                "link": question["link"],
                "owner_reputation" : question["owner_reputation"],
                "score" :  question["score"],
                "tags" : question["tags"],

            }

    def insert_question_to_db():
        """ Inserts a new question to the database """ 
        insert_question_query = """
            INSERT INTO public.questions(
                question_id,
                title,
                is_answered,
                link,
                owner_reputation,
                score,
                tags
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
        rows = call_stack_overflow_api()
        for row in rows:
            row = tuple(row.values())
            #using posthres hook
            pg_hook = PostgresHook(postgres_conn_id = "postgres_connection")
            pg_hook.run(insert_question_query, parameters = row)


    def filter_questions() -> str:
        """
        read all questions from a database and filter them.
        Return a JSON string that looks like:

        [
            {

            "title" : "Question Title",
            "Is_answered" : False,
            "Link": "https://stackoverflow.com/questions/000001/...",
            "tag" : ["tag_a","tag_b"],
            "Question_id" : 000001
            },

        ]
        """

        columns = ("title", "is_answered", "link", "tags", "question_id")
        filtering_query = """
            SELECT title, is_answered, link, tags, question_id
            FROM public.questions
            WHERE score >= 1 AND owner_reputation > 1000;
            """
        pg_hook = PostgresHook(postgres_conn_id = "postgres_connection").get_conn()

        with pg_hook.cursor("server.Cursor") as pg_cursor:
            pg_cursor.execute(filtering_query)
            rows = pg_cursor.fetchall()
            results = [dict(zip(columns,row)) for row in rows]
            return json.dumps(results, indent=2)


    def write_question_to_s3():
        hook = S3Hook(aws_conn_id = "s3_connection")
        hook.load_string(
            string_data = filter_questions(),
            key = S3_FILE_NAME,
            bucket_name = Variable.get("S3_BUCKET"),
            replace = True,
        )


    def render_template(**context):
        """Render HTML template using questions metadata from s3 bucket """

        hook = S3Hook(aws_conn_id = "s3_connection")
        file_content = hook.read_key(
            key = S3_FILE_NAME, bucket_name = Variable.get("S3_BUCKET")
        )
        questions = json.loads(file_content)

        root = os.path.dirname(os.path.abspath(__file__))
        env = Environment(loader=FileSystemLoader(root))
        template = env.get_template("email_template.html")
        html_content = template.render(questions = questions)

        # Push rendered HTML as a string to the Airflow metadata database
        # to make it available for the next task 

        # Use XCOM to pass HTML content to the next task 
        task_instance = context['task_instance']
        task_instance.xcom_push(value = html_content, key = "html_content")