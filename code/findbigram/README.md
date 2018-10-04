## Exercise 3

Write a MapReduce program in Hadoop that computes the single most common bigram (pair of adjacent words) in the input. The key idea is to count the number of occurrences of all bigrams in a collection of text documents and to output the bigram with the highest number of occurrences.

### Input

* Download the input file [snippets.zip](../../data/snippets.zip)
* Unzip it: `unzip snippets.zip`
* The `snippets` folder contains 782 text files with name `lineXXX`, where `XXX` runs from `000` to `781`.
* Each input file contains a text on multiple lines, including numbers, punctuation, etc.

### Algorithm

The idea is to split every line in every document into "tokens" (i.e., space-delimited sequences of characters) and count, for every token and the next or previous token, how many times it is found in the input.

### Note 

In your driver code, set the number of reducers to 1. This trick will help you to write a single Hadoop MapReduce program.

### Output

The output should contain one line in the following format:

    <bigram><TAB><count>

where `word` is the most common bigram in the input files, `<TAB>` is the TAB character, and `count` is an integer greater than 0 representing the number of occurrence of `bigram` in the input files.

### Hint

Another why to split a Java string into tokens is reported in the following code, adapt it as necessary:

```java
String line = "aaaa bbbb";
String[] words = line.split("\\s+");
```

### Extra

Modify your program to compute the top 10 most frequent bigrams.