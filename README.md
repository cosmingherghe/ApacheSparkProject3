# Pretty comprehensive project where you got practice extracting out the different fields from the Jason document and how partitions work

Transformations in The Spark Land where we're trying to combine two data sets together in a set oriented way. We were either doing a union intersection or a subtraction.

- union " df1.unionByName(df2); " - it takes the unique records from dataset 1 and unique records from dataset 2 and combines them together.
- intersect " df1.intersect(df2); " - only the records that are common among both dataset 1 and dataset 2 would make it into the data frame.
- except " df1.except(df2) " - only the unique records in data set 2 would be accounted for in the final data frame.
