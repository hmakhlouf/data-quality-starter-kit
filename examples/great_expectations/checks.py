"""Same rules with Great Expectations (GX Core 1.x) on a pandas DataFrame.

pip install great_expectations pandas
For a SQL database, use context.data_sources.add_postgres(...) / add_snowflake(...) instead of pandas.
"""
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

df = pd.read_csv("output/tgt_program_records.csv")  # export of the target table

context = gx.get_context()
batch = (context.data_sources.add_pandas("demo")
         .add_dataframe_asset("tgt_program_records")
         .add_batch_definition_whole_dataframe("all")
         .get_batch(batch_parameters={"dataframe": df}))

suite = context.suites.add(gx.ExpectationSuite(name="tgt_program_records"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="record_id"))
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="record_id"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="program_id"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="category"))
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["open", "closed", "pending"]))
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0))
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="program_id", value_set=["P01", "P02", "P03", "P04", "P05"]))

result = batch.validate(suite)
print(f"success={result.success}")
for r in result.results:
    if not r.success:
        print(" FAIL", r.expectation_config.type, r.expectation_config.kwargs.get("column"),
              r.result.get("unexpected_count"))
