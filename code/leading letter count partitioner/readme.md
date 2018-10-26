Exercise
=========

Write a MapReduce job that outputs the most common word that starts with a vowel and the most common word that starts with a consonant. The output should also include the number of times the words appear.

	One way to solve this problem would be to use vowel/consonant as the key and pack the words and their counts into the values. Doing that will make using a combiner harder (why?), so the values emitted from the map phase should contain only numeric word counts.

	**Hint: try implementing a custom partitioner.**

	The following code show the partitioner we need to implement. The `getPartition` method is passed the key, value and the number of partitions:

	```
	public interface Partitioner<K2, V2> extends JobConfigurable {
		int getPartition(K2 key, V2 value, int numPartitions);
	}
	```
