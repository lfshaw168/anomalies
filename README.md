Name: Leo Shaw
Date: 7-5-17

Submission for Insight Data Engineering

The anomaly detection challenge was reduced to a breadth-first graph search for the last T transactions conducted by the Users within D degrees of the current User. Transactions were stored in a "ledger" belonging to each User. By doing a breath-first traversal of the friends of each User and recording the distance (degree) from the original User, we can aggregate all friend Users within D degrees from the User of interest. From this Set of Users, we can look at the last Transaction of each User and use a heap to identify the latest Transaction in O(log k) time, where k is the number of Users in the Set. To aggregate T Transactions, the total time is O(T log k). This final set of T Transactions is then traversed to calculate the mean and standard deviaion to compare against the current incomign Transaction.

Dependencies: json-simple (https://code.google.com/archive/p/json-simple/; this was used to simplify the retrieval of data from the JSON objects)

Shell script: the source files were compiled into a .jar file, which can be called in the command line with the "-jar" flag. The included shell script is pre-filled with the appropiate call. # anomalies
