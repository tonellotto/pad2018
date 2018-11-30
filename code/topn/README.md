## Exercise 11

Write a MapReduce program in Hadoop that, given a set of (`key-as-string`, `value-as-integer`) pairs, say we want to create a top N (where N > 0) list.
Assume that all input keys are unique. That is, for a given input set {(K, V)}, all Ks are unique.

### Algorithm

The MapReduce program is pretty straightforward: each mapper will find a local top N list (for N > 0) and then will pass it to a *single* reducer. Then the single reducer will find the final top N list from all the local top N lists passed from the mappers. Each mapper creates a local top 10 list and then emits the local top 10 to be sent to the reducer. In emitting the mappers’ output, we use a single reducer key so that all the mappers’ output will be consumed by a single reducer.

### Output

The output should contain one line per word in the following format:

    <word><TAB><count>

where `word` is a space-delimited sequence of characters appearing at least once in the input files, `<TAB>` is the TAB character, and `count` is an integer greater than 0 representing the number of occurrence of `word` in the input files.

### Hint

To parameterize the top N list, we just need to pass the N from the driver (which launched the MapReduce job) to the `map()` and `reduce()` functions by using the MapReduce `Configuration` object. The driver sets the "`top.n`" parameter and `map()` and `reduce()` read that parameter, in their `setup()`functions.

    public class TopN_Mapper { 
        private SortedMap<Double, Text> top10cats = new TreeMap<Double, Text>();
        private int N = 10; // default is top 10

        protected void setup(Context context) {
            // "top.n" has to be set up by the driver of our job
            Configuration conf = context.getConfiguration();
            N = conf.get("top.n");
        }
