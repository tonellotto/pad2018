Exercise on Secondary Sort
==========================

Hadoop’s MapReduce platform sorts the keys, but not the values. (note: Google’s MapReduce platform explicitly supports secondary sorting). *Secondary sort* is useful when we want some of the values for a unique map key to arrive at a reducer in a fixed order. Essentially, we want the reducer's values iterator to be sorted. The value of secondary sorting is fundamental for some MapReduce computations, such as the friends-of-friends algorithm.

Suppose we have a file with a bunch of line-separated people's names, and we want to sort them. We want our reducer to receive names

-	partitioned by last name, and
-	sorted by first name.

This is actually somewhat difficult to do, since we want to partition by key, but sort the reducer’s values iterator.

Secondary sorts require an understanding of both data arrangement and data flows in MapReduce. There are the three elements that impact data arrangement and flow (partitioning, sorting, and grouping) and how they’re integrated into MapReduce.

1.	The **partitioner** is invoked as part of the map output collection process, and is used to determine which reducer should receive the map output. After partitioning, each partition is locally written in a separate *spill*.

2.	The sorting **comparator** locally sorts the map outputs within their respective partitions. After the (locally-sorted) spills are moved to the reducers, they are merged in consolidated partitions and sorted again.

3.	The **grouping** occurs when the reduce phase is streaming map output records from local disk. Grouping is the process by which you can specify how records are combined to form one logical sequence of records for a reducer invocation.

Custom key
----------

The first thing we need is to create a composite output key, which will be emitted by map functions. The composite key will contain two parts:

1.	The *natural key*, which is the key to use for joining purposes
2.	The *secondary key*, which is the key to use to order all of the values sent to the reducer for the natural key

As we know, a custom key in Hadoop must be writable and comparable, i.e., our composite output key (let call it `Person`) must implement the `WritableComparable<Person>` [interface](https://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/io/WritableComparable.html).

```
public class MyWritableComparable implements WritableComparable {
	// Some data
	private int counter;
	private long timestamp;

	@Override // To be implemented in your class
	public void write(DataOutput out) throws IOException {
		out.writeInt(counter);
		out.writeLong(timestamp);
	}

	@Override // To be implemented in your class
	public void readFields(DataInput in) throws IOException {
		counter = in.readInt();
		timestamp = in.readLong();
	}

	@Override // To be implemented in your class
	public int compareTo(MyWritableComparable w) {
		int thisValue = this.value;
		int thatValue = ((IntWritable)o).value;
		return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}
}
```

Custom partitioner
------------------

The partitioner has a single function that determines which partition (reducer) your map output should go to. The default MapReduce partitioner (`HashPartitioner`) calls the `hashCode` method of the output key and performs a modulo with the number of reducers to determine which reducer should receive the output. The default partitioner use the entire key, which won't work for our composite key, because it will likely send keys with the same natural key to different reducers. Instead, you need to write your own partitioner, *which partition on the natural key only*.

The following code show the partitioner we need to implement. The `getPartition` method is passed the key, value and the number of partitions:

```
public interface Partitioner<K2, V2> extends JobConfigurable {
	int getPartition(K2 key, V2 value, int numPartitions);
}
```

Our partitioner will calculate a hash based on the last name in the `Person` class, and perform a modulo of that with the number of partitions (which is the number of reducer).

Custom sorting
--------------

Both the map and the reduce participating in sorting. The map-side sorting is an optimization to help the reducer sorting more efficiently. We want MapReduce to use the entire key for sorting purposes, which will order key according to both the last name and the first name.

```
public class PersonComparator extends WritableComparator
{
	protected PersonComparator() {
		super(Person.class, true);
	}

	@Override // To be implemented in your class
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		return 0; // To change, just a placeholder
	}
}
```

Custom grouping
---------------

When you’re at the grouping stage, all of the records are already in secondary-sort order, and the grouping comparator needs to bundle together records with the same last name.

```
public class PersonNameComparator extends WritableComparator
{
	protected PersonNameComparator() {
		super(Person.class, true);
	}

	@Override // To be implemented in your class
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		return 0; // To change, just a placeholder
	}
}
```

MapReduce job
-------------

The final steps involve telling MapReduce to use the custom key, partitioner, sort comparator, and group comparator classes:

```
job.setMapOutputKeyClass(Person.class);
job.setPartitionerClass(PersonNamePartitioner.class);
job.setSortComparatorClass(PersonComparator.class);
job.setGroupingComparatorClass(PersonNameComparator.class);

```
