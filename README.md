![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Sequential Pattern Mining with Spark

Sequential pattern mining is an important mining technique with a wide range of real-life applications.
It has been found very useful in domains such as 

* market basket analysis
* marketing strategy 
* medical treatment
* natural disaster
* user behavior analysis

and more.

It is an extension to the concept of association rule mining and solves the problem of discovering statistically 
relevant patterns in big datasets that specify (timely ordered) sequences of data.

**Sequences from Transactions**

In market basket analysis, a sequence is a list of different transactions of a certain customer, ordered by the transaction time. Each transaction is a collection of the items a particular customer ordered (itemset). 

Sequential Patterns are very interesting in marketbasket analysis as they specify inter-transaction correlations, and e.g. discover which items are frequently bought one after another. For marketers, this is a valuable insight from which they may predict which item will be probably bought next.


**Sequences from Web Sessions**

In the context of web mining, especially web usage mining, companies need to understand what motivates their customers to purchase and how to influence the buying process to develop successful promotional activities.

Evaluating web sessions and the timely ordered sequences of page visits (within a certain time period), e.g. helps to understand similarities of click-streams much better than treating sessions as sets of page visits. As a results, visitors can be clustered or segmented not only by visited content, but also by their timely behavior and signatures.

## Similarity of Sequential Patterns

Computing the similarity of sequental patterns is an important task to find regularities in sequences of data. This is the key to understand customer behavior, build profiles and signatures, and also to group similar customers by their temporally behavior.    
