from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import T

# Define a Python function for forward filling
def forward_fill(values):
    filled_values = []
    last_valid = None
    for value in values:
        if value is not None and value != '.':
            last_valid = float(value)
        filled_values.append(last_valid)
    filled_values[0] = filled_values[1]  # Replace the first value with the second value
    return filled_values

# Register the UDF with Snowflake
forward_fill_udf = udf(forward_fill, return_type=T.ArrayType(T.FloatType()), input_types=[T.ArrayType(T.StringType())])