import re
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Define your custom DAG parser function
def extract_tables_and_sql_statements_from_dag(dag):
    created_tables = set()
    other_tables = set()
    sql_statements = []

    for task in dag.tasks:
        if isinstance(task, (PythonOperator, BashOperator)):
            if isinstance(task, PythonOperator):
                # Extract tables and SQL statements from PythonOperator
                python_code = task.python_callable.__code__.co_code.decode('utf-8')
                # Assuming SQL statements are enclosed in triple-quotes
                sql_statements += re.findall(r'(\'\'\'(.*?)\'\'\'|"""(.*?)""")', python_code, re.DOTALL)

            elif isinstance(task, BashOperator):
                # Extract tables and SQL statements from BashOperator
                bash_command = task.bash_command

                # Assuming SQL queries/statements are enclosed in backticks or $()
                sql_queries = re.findall(r'\b`(.+?)`\b', bash_command)

                for sql_query in sql_queries:
                    # Assuming tables are referenced as "schema_name.table_name"
                    table_references = re.findall(r'\b\w+\.\w+\b', sql_query)

                    # Check if it's a CREATE TABLE statement
                    if re.search(r'\bCREATE\s+TABLE\b', sql_query, re.IGNORECASE):
                        created_tables.update(table_references)
                    else:
                        other_tables.update(table_references)

    return created_tables, other_tables, sql_statements

# Define your DAG
dag = DAG(
    'my_custom_dag',
    default_args={
        'owner': 'airflow',
        'start_date': '2023-01-01',
    },
    schedule_interval=None,  # Set your schedule interval here
)

# Define your tasks within the DAG
task1 = PythonOperator(
    task_id='python_task',
    python_callable=my_python_function,
    dag=dag,
)

task2 = BashOperator(
    task_id='bash_create_task',
    bash_command="CREATE TABLE my_schema.my_table (id INT, name STRING)",
    dag=dag,
)

task3 = BashOperator(
    task_id='bash_insert_task',
    bash_command="INSERT INTO my_schema.my_table (id, name) VALUES (1, 'John')",
    dag=dag,
)

# Add more tasks to your DAG as needed

# Extract created and other tables, along with SQL statements, from the DAG
created_tables, other_tables, sql_statements = extract_tables_and_sql_statements_from_dag(dag)

# Print the created tables
print("Tables Created in the DAG:")
for table in created_tables:
    print(table)

# Print the other tables referenced in the DAG
print("\nOther Tables Referenced in the DAG:")
for table in other_tables:
    print(table)

# Print the extracted SQL statements
print("\nSQL Statements in the DAG:")
for sql_statement in sql_statements:
    print(sql_statement)