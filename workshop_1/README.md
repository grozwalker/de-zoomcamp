* Question 1: What is the sum of the outputs of the generator for limit = 5?

Sum: 8.382332347441762


* Question 2: What is the 13th number yielded by the generator?

3.605551275463989

* Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.

353

```
# to do: homework :)
import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='gen1')


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

person_1 = []
for person in people_1():
    person_1.append(person)


person_2 = []
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    person_2.append(person)


# we can load any generator to a table at the pipeline destnation as follows:
info_1 = generators_pipeline.run(person_1,
										table_name="persons",
										write_disposition="append")


info_2 = generators_pipeline.run(person_2,
										table_name="persons",
										write_disposition="append")
```

```
# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data

print("\n\n\n persons table below:")

persons = conn.sql("SELECT sum(age) FROM persons").df()
display(persons) #353
```


* Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.

266

```
# to do: homework :)
import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='gen2')


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

person_1 = []
for person in people_1():
    person_1.append(person)


person_2 = []
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    person_2.append(person)


# we can load any generator to a table at the pipeline destnation as follows:
info_1 = generators_pipeline.run(person_1,
										table_name="persons",
										write_disposition="merge",
                    primary_key="ID")


info_2 = generators_pipeline.run(person_2,
										table_name="persons",
										write_disposition="merge",
                    primary_key="ID")
```

```
# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data

print("\n\n\n persons table below:")

persons = conn.sql("SELECT sum(age) FROM persons").df()
display(persons)

# print("\n\n\n stream_download table below:")

# passengers = conn.sql("SELECT * FROM stream_download").df()
# display(passengers) # 266

```
