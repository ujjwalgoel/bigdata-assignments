###Answer 0

       For both the implementations I used two jobs. First jobs processes the relevant pairs of words and emits the counts for them while the second one just has a mapper with a reducer. This mapper processes the intermediate data emitted by the first job, computes the frequencies of each word and populates this information in a map. The map method then computes the PMI values using these frequencies and emits the values.  


   Stripes Implementation
   =====================

   First Job
   ---------

The mapper looks for pairs which occur in a line. The key is each word and value is a map of each word that occurs with this word in a 
line and its count. It also emits the * key with aggregate word count. This is to keep track of word frequency in the document.
The reducer combines these maps of word counts based on word as key to emit aggregate map. The intermediate file output looks like a tab 
separated key value pair, where each key is a word and value is a word count map that occurs with that word.

   Second Job
   ----------

The setup method processes these intermediate files and populates a global count map for each word. This is needed because P(X,Y) would 
need P(X) and P(Y) as well as count of total words in the document. The map method of the second then iterates over these intermediate 
files and computes PMI for each pair and emits them. The final output is a pair of words with associated PMI value in each line.  


   Pairs Implementation
   ====================

   First Job
   ---------

The mapper emits each occurence of a word pair as key and ONE as the value. It also emits (word,*) as key to compute the aggregate count 
per word and on the global level. The reducer combines these word pairs to get aggregate on each pair level. I have used a hash partitioner 
to partition based on first word in the pair so that the keys come in the sorted order of first word in each pair. Hence (word,*) is the 
first one in each pair. The intermediate file output looks like a tab separated key value pair where each key is a word pair and value is 
a word count.

    Second Job
    ----------

The setup method processes these intermediate files and computes a global count map for each word. The map method then iterates over each 
line, computes PMI value for each pair using the values from the global count map and the frequency of that pair. The final output is a pair 
of words with associated PMI value in each line.



I used a separate java program to compute the answers to the questions.



###Answer 1 

With Combiners (ran on UMIACS cluster)
Stripes - 52.616 + 19.253 = 71.869 seconds
Pairs - 92.26 + 20.154 = 112.414 seconds


###Answer 2

Without Combiners (ran on UMIACS cluster)
Stripes - 111.345 + 21.175 = 132.52
Pairs - 113.393 + 21.198 = 134.591 seconds

###Answer 3

Total Distinct Word Pairs : 168046

###Answer 4

Pair with highest PMI : caul, kidneys
I am surprised to see this pair as highest occuring. I thought it would have been an article like a or the occuring with another very 
frequently occuring word. I think it could be because they both refer to body parts.

###Answer 5

Word with highest PMI with Cloud : tabernacle with value : 2.7675748
Word with second highest PMI with Cloud : pass with value : 2.0844972
Word with third highest PMI with Cloud : upon with value : 1.6790766

###Answer 6

Word with highest PMI with Love : hate with value : 2.0180273
Word with second highest PMI with Love : hermia with value : 1.8421845
Word with third highest PMI with Love : lysander with value : 1.7828803

