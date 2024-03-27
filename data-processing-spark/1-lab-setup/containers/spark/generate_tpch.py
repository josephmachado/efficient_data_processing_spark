from jinja2 import Template

# Generate DDL and DML query for creating and loading data into TPCH
# Generate count(*) queries for all the tables to check load

TABLE_SCHEMAS = {
    'customer': {
        'c_custkey': 'Long',
        'c_name': 'String',
        'c_address': 'String',
        'c_nationkey': 'Long',
        'c_phone': 'String',
        'c_acctbal': 'Double',
        'c_mktsegment': 'String',
        'c_comment': 'String',
    },
    'lineitem': {
        'l_orderkey': 'Long',
        'l_partkey': 'Long',
        'l_suppkey': 'Long',
        'l_linenumber': 'Long',
        'l_quantity': 'Double',
        'l_extendedprice': 'Double',
        'l_discount': 'Double',
        'l_tax': 'Double',
        'l_returnflag': 'String',
        'l_linestatus': 'String',
        'l_shipdate': 'String',
        'l_commitdate': 'String',
        'l_receiptdate': 'String',
        'l_shipinstruct': 'String',
        'l_shipmode': 'String',
        'l_comment': 'String',
    },
    'nation': {
        'n_nationkey': 'Long',
        'n_name': 'String',
        'n_regionkey': 'Long',
        'n_comment': 'String',
    },
    'orders': {
        'o_orderkey': 'Long',
        'o_custkey': 'Long',
        'o_orderstatus': 'String',
        'o_totalprice': 'Double',
        'o_orderdate': 'String',
        'o_orderpriority': 'String',
        'o_clerk': 'String',
        'o_shippriority': 'Long',
        'o_comment': 'String',
    },
    'part': {
        'p_partkey': 'Long',
        'p_name': 'String',
        'p_mfgr': 'String',
        'p_brand': 'String',
        'p_type': 'String',
        'p_size': 'Long',
        'p_container': 'String',
        'p_retailprice': 'Double',
        'p_comment': 'String',
    },
    'partsupp': {
        'ps_partkey': 'Long',
        'ps_suppkey': 'Long',
        'ps_availability': 'Long',
        'ps_supplycost': 'Double',
        'ps_comment': 'String',
    },
    'region': {
        'r_regionkey': 'Long',
        'r_name': 'String',
        'r_comment': 'String',
    },
    'supplier': {
        's_suppkey': 'Long',
        's_name': 'String',
        's_address': 'String',
        's_nationkey': 'Long',
        's_phone': 'String',
        's_acctbal': 'Double',
        's_comment': 'String',
    },
}


def create_ddl_dml_queries(tbl_schemas=TABLE_SCHEMAS):
    sql_template = Template(
        """
    DROP TABLE IF EXISTS {{ table_name }};

    CREATE TABLE {{ table_name }} (
        {% for column_name, data_type in columns.items() %}
        {{ column_name }} {{ data_type }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '"'
    STORED AS TEXTFILE;

    LOAD DATA LOCAL INPATH
     '/opt/spark/work-dir/tpch-dbgen/{{ table_name }}.tbl'
     OVERWRITE INTO TABLE {{ table_name }};
    """
    )

    with open("setup.sql", 'w+') as f:
        for table_name, column_name_type in tbl_schemas.items():
            f.write(
                sql_template.render(
                    table_name=table_name, columns=column_name_type
                )
            )


def create_count_queries(tables):
    with open("count.sql", 'w+') as f:
        f.writelines([f'SELECT COUNT(*) FROM {table}; \n' for table in tables])


if __name__ == '__main__':
    create_ddl_dml_queries()
    create_count_queries(TABLE_SCHEMAS.keys())
