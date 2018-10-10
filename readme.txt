In this project I have used Hadoop map reduce to carry out certain queries on a social network. The data contains 2 files, first is the network itself, represented in form of adjacency list and second is the information of each user. (Total 50000 users)

MutualFriends.java - extracts all mutual friends between any 2 given friends.

TopTenMutualFriends - extracts pairs of friends whose mutual friend count is in top 10. 

MutualFriendNameState - In memory join to output the name and state of mutual friends given any 2 friends pair.

MinAgedDirectFriend - extracts pairs of friends whose min aged direct friend is in the oldest top 10.


To run the files, it is expected that hadoop setup is complete and working, put the input file in hadoop file system and run the map reduce job.