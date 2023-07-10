from datetime import datetime as dt
from bdq import get_latest_records, get_latest_records_with_pk_confict_detection_flag
from bdq import spark

def test_gets_latest_records():
  increment_data =  [
    (1, dt(2000, 1, 1, 0, 0, 0), "1001")
    ,(1, dt(2000, 1, 1, 2, 0, 0), "1002")
    ,(2, dt(2000, 1, 1, 0, 0, 0), "2001") #carbon copy duplicate
    ,(2, dt(2000, 1, 1, 0, 0, 0), "2001") #carbon copy duplicate
    ,(3, dt(2000, 1, 1, 0, 0, 0), "3001")
    ,(3, dt(2000, 1, 1, 2, 0, 0), "3002#1") #pk conflict, two entries with the same technical fields, but different atributes, which one to chose?
    ,(3, dt(2000, 1, 1, 2, 0, 0), "3002#2") #pk conflict
  ]

  df = spark.createDataFrame(increment_data, "pk:int,change_ts:timestamp,attr:string")
  display(df)

  primary_key_columns = ['pk']
  order_by_columns = ['change_ts']

  # will randomly chose one pk in case of conflict, without any notification about conflicts
  simple_get_latest_df = get_latest_records(df, primary_key_columns, order_by_columns)
  display(simple_get_latest_df)

  # will flag records which cause conflicts and cannot be resolved
  # bad records need to be quarantined and handled in special process
  conflict_get_latest_df = get_latest_records_with_pk_confict_detection_flag(df, primary_key_columns, order_by_columns)
  display(conflict_get_latest_df)