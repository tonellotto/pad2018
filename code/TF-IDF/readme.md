# Playing with TF-IDF

## Introduction

Suppose we have a set of English text documents and wish to determine which
document is most relevant to the query "*the brown cow*". A simple way to start
out is by eliminating documents that do not contain all three words "*the*",
"*brown*" and "*cow*", but this still leaves many documents. To further distinguish
them, we might count the number of times each term occurs in each document and
sum them all together; the number of times a term occurs in a document is called
its **term frequency**. However, because the term "*the*" is so common, this will tend
to incorrectly emphasize documents which happen to use the word "*the*" more,
without giving enough weight to the more meaningful terms "*brown*" and "*cow*".
Also the term "*the*" is not a good keyword to distinguish relevant and
non-relevant documents and terms like "*brown*" and "*cow*" that occur rarely are
good keywords to distinguish relevant documents from the non-relevant documents.
Hence an **inverse document frequency** factor is incorporated which diminishes the
weight of terms that occur very frequently in the collection and increases the
weight of terms that occur rarely.

The *term count* in the given document is simply the number of times a given
term appears in that document. This count is usually normalized to prevent a
bias towards longer documents (which may have a higher term count regardless of
the actual importance of that term in the document) to give a measure of the
importance of the term ti within the particular document dj. Thus we have the
**term frequency**, defined as TFij = nij / Ni, where nij is the number of
occurrences of the considered term (ti) in document dj, and the denominator Nj
is the sum of number of occurrences of all terms in document dj (the document
length).

The **inverse document frequency** is a measure of the general importance of the
term (obtained by dividing the total number of document by the number of
documents containing the term, and then taking the logarithm of that quotient):
IDFi = log(|D|/Mi), where |D| is the total number of documents in the corpus,
and M_i is the number of documents where the term ti appears in.

Then (TF-IDF)ij = TFij x IDFij.

A high weight in TF-IDF is reached by a high term frequency (in the given
document) and a low document frequency of the term in the whole collection of
documents; the weights hence tend to filter out common terms. The TF-IDF value
for a term will always be greater than or equal to zero.

## Compute TF-IDF using MapReduce

Given a small collection of documents, we are going to implement TF-IDF scores using Hadoop. We will need the following information:

* number of times term ti appears in a given document (n)
* number of terms in each document (N)
* number of documents term ti appears in (m)
* total number of documents (|D|)

We use multiple rounds of Map/Reduce to gradually compute TF-IDF:

1. **Word frequency in document**: starting from a directory containing a set of
text files, we will produce another set of files associating the couple (ti, dj)
to the number n of times the term ti appears in the document dj.

	*Mapper*: input: (doc name, doc contents), output: ((word, doc name), 1)
	
	*Reducer*: sums counts for word in document, outputs ((word, doc name), n)
	
2. **Word count in document**: starting from the directory containing the output of
the previous job, we will produce another set of files associating the couple
(ti, dj) to the couple (n, N), where N represents the total number of terms in
the document dj.

	*Mapper*: input: ((word, doc name), n), output: (doc name, (word, n))
	
	*Reducer*: sums frequencies n for individual terms in the same document, outputs ((word, doc name), (n, N))

3. **Word frequency in collection**: starting from the directory containing the
output of the previous job, we will produce another set of files associating the
couple (ti, dj) to the triple (n, N, m), where m represents the number of
documents the term ti appears in the document dj.

	*Mapper*: input: ((word, doc name), (n, N)), output: (word, (doc name, n, N))
	
	*Reducer*: sums counts for word in collection, output: ((word, doc name), (n, N, m))

4. **Calculate TF-IDF**: starting from the directory containing the output of
the previous job, and assuming |D| is known, we will produce another set of
files associating the couple (ti, dj) its TF-IDF score.

	*Mapper*: input: ((word, doc name), (n, N, m)), output: ((word, doc name), TF-IDF)

	*Reducer*: do nothing, output: ((word, doc name), tf-idf)

## Word frequency in document 

This phase is designed in a job whose task is to count the number of words in
each of the documents in the input directory. The mapper will receive as input a
key that is, by default, the byte offset of the current line in the current file
processed (`LongWritable` object) and a value that is the line read from the file
(`Text` object). It must output another (key, value) pair. The problem is coding
the (word, doc name) pair in a single object. While it is possible to implement
a custom class to do the job, we will use a “string trick”, emitting a simple
string composed by the word, the special character “`@`” and the doc name. In
order to obtain the document name from the `Context` object, use the following
statement: 

	String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();

Then, use the following statements to remove punctuation and other word
anomalies: 

	Pattern p = Pattern.compile("\\w+"); 
	Matcher m = p.matcher(value.toString()); 
	while (m.find()) { 
		String word = m.group().toLowerCase(); 
		// remaining code 
	} 

During the mapper execution, each word in the line should be lower-cased, and
ignored if it does not start with a letter or if it contains the character “`_`”.

The reducer behaves as the standard, well-known `WordCount` reducer. In this case,
keys are represented by `Text` objects, and values by `IntWritable` objects. The
output will be a set of files (one per reducer): each line of each file will
contain a word@document string, a tab character and an integer coded as string.
Please remember that Hadoop requires the same classes for keys in the mapper
output and the reducer input, as well as the same classes for relative values,
although key and value classes can be different.

## Word count in document

The goal of this phase is to count the total number of words for each document,
in a way to compare each word with the total number of words. The mapper will
receive as input a key that is, by default, the byte offset of the current line
in the current file processed (`LongWritable` object) and a value that is the line
read from the file (`Text` object). It must output another (key, value) pair. The
problem is again coding the (word, n) pair in a single object. We will use the
“string trick”, emitting a simple string composed by the word, the special
character “`/`” and a string representing the number of occurrences of the word in
the document. In order to split a string line in an array of strings with a
custom char you can use the following statement: 

	String[] tokens = line.split("@"); 

The reducer will receive two `Text` objects as keys and values representing the
(doc name, (word, n) couple. It just needs to sum the total number of values in
a document and pass this value over to the next step, along with the previous
number of keys and values, as necessary data for the next step, writing in the
files a line for each ((word, doc name), (n, N)) couple’s couple.