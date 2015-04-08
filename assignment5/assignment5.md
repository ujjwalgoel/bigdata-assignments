###Answer 1
The problem with this HBase design is that we insert one row at a time into the HBase table 
which would be really slow in case of large scale dataset. Similarly, while reading we fetch 
only one term at a time which is not optimal.


###Answer 2
Instead we should use bulk-writes to the HBase table in the BuildIndex class. Use a Put variable 
in the Reducer class and add multiple rows to Put before flushing it down to HBase. The number 
of rows to flush at once can be tuned to achieve optimal performance. In case of BooleanRetrieval 
class, we can fetch multiple terms in the search string at once from HBase to optimize read 
performance.
