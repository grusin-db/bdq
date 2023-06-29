from bdq.functions import surrogate_key_hash, surrogate_key_string
from bdq import fact_dim_broken_relationship
from bdq import spark

fact_df = spark \
  .createDataFrame([
    [ "Grzegorz", "IT", "EU" ],
    [ "Mark", "IT", "EU" ],
    [ "Justin", "IT  ", "EU    " ],
    [ "Alice1", "IT", "USA" ],
    [ "Alice2", "IT", "USA" ],
    [ "Alice3", "IT", "USA" ],
    [ "Alice4", "IT", "USA" ],
    [ "Alice5", "IT", "USA" ],
    [ "Bob", "Sales", "USA" ],
    [ "Bob2", "Sales", "USA" ],
    [ "Bob3", "Sales", "USA" ]
  ], "Name:string,Dept:string,Country:string") \
  .withColumn("dept_fk", surrogate_key_hash(["Dept", "Country"], rtrim=True))

print("fact:")
display(fact_df)

dim_df = spark \
  .createDataFrame([
    ["IT", "EU", "European IT department"],
    ["Sales", "USA", "USA Sales dept."]
  ], "department:string,cntry:string,comment:string") \
  .withColumn("dept_pk", surrogate_key_hash(["department", "cntry"], rtrim=True))

print("dim:")
display(dim_df)

broken_df = fact_dim_broken_relationship(
  fact_df=fact_df,
  fk_columns=["Dept", "Country"],
  dim_df=dim_df,
  pk_columns=["department", "cntry"],
  sample_broken_records=2
)

display(broken_df)

broken_sk_df = fact_dim_broken_relationship(
  fact_df, ["dept_fk"],
  dim_df, ["dept_pk"]
)

display(broken_sk_df)