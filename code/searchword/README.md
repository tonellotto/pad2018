## Exercise 4

Write a MapReduce program in Hadoop that implements a simple "find" algorithm. The key idea is to filter out the contents where an input word does not appear.

### Input

* Download the input file [snippets.zip](../../data/snippets.zip)
* Unzip it: `unzip snippets.zip`
* The `snippets` folder contains 782 text files with name `lineXXX`, where `XXX` runs from `000` to `781`.
* Each input file contains a text on multiple lines, including numbers, punctuation, etc.

### Algorithm

The idea is check if every line contains or not the input word, and output only the lines containing the input words.

### Output

The output should contain one string per line:

    <line>

where `line` represents an uppercase line of the input files. Ordering of lines as in input files is not mandatory.

