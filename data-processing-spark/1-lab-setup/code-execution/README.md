The code in the book is available at the [Efficient Data Processing in Spark ](https://github.com/josephmachado/efficient_data_processing_spark/tree/main)GitHub repo.

The code is organized as follows

```bash
data-processing-spark/
└── section-name/
    └── chapter-name/
        └── header-name/
            └── keyword.sql
            └── keyword.py
            └── exercise-[1-n].sql
            └── exercise-[1-n].py
```

The code examples and exercises will have both Spark SQL and Pyspark code (as applicable).

it is recommend that you open a Spark SQL session with `make sql` to run the spark sql code.

For the pyspark code it is recommended that you refer the code under **run_code** function and execute this within a Pyspark session (started with the `make pyspark` command).