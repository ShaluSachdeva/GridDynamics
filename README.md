<<<<<<<<<<<<<--------TASK 1 ------------>>>>>>>>>>>>>>>>>>>>>>>>>

Task 1 is done based on assumption that the most recent entries are with recent date and max score.
Since in your sample output, it is shown that third last entry with score 100 is not recent but with same user id and date 
with score 300, it is marked most recent.


Output example:
scala> markMostRecent(dataset).show(8)
+---------+-----------+-------------------------+
|user_id  | date      |score       | status     |
+---------+-----------+-------------------------+
|1        | 2018-05-14| 10         |		    |
|2        | 2018-05-14| 10         |		    |
|1        | 2018-05-15| 20         |		    |
|2        | 2018-05-16| 20         |		    |
|1        | 2018-05-16| 100        |		    |
|2        | 2018-05-17| 100        |		    |	
|1        | 2018-05-17| 300        | most_recent|
|2        | 2018-05-17| 200        | most_recent|
+---------+-----------+-------------------------+

<<<<<<<<<<<<<--------TASK 2 ------------>>>>>>>>>>>>>>>>>>>>>>>>>

Task 2 is tricky in the sense there are no primary keys and getting only deleted entries without primary keys are not possible..

So, I got the Updated entries, Inserted Entries but Deleted contain both delted and updated.