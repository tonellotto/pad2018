## Exercise 2

Write a MapReduce program in Hadoop that transforms the content of text files in uppercase.

### Input

* Download the input file [snippets.zip](../../data/snippets.zip)
* Unzip it: `unzip snippets.zip`
* The `snippets` folder contains 782 text files with name `lineXXX`, where `XXX` runs from `000` to `781`.
* Each input file contains a text on multiple lines, including numbers, punctuation, etc.

### Algorithm

The idea is to transform every line in every document into from whatever case to upper case.

### Note
In your driver code, set the number of reducers to 2.

### Output

The output should contain one string per line:

    <line>

where `line` represents an uppercase line of the input files. Ordering of lines as in input files is not mandatory.
You can download from HDFS a local copy of the output data by using the command:
```bash
hadoop fs −getmerge <output>
```
where `<output>` is the name of the output folder on HDFS.
### Hint

To transform a Java string into uppercase you can use the following code, adapted as necessary:
```java
...
String line = "aaaa bAAAbbb";
String LINE = line.toUpperCase();
...
```
