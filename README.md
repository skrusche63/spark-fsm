![Elasticworks.](https://raw.githubusercontent.com/skrusche63/spark-fsm/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of predictive analytics to Elasticsearch.

### Reactive Series Analysis Engine

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

![Spark-FSM](https://raw.githubusercontent.com/skrusche63/spark-fsm/master/images/spark-fsm.png)

#### Market Basket Analysis

In market basket analysis, a sequence is built from the transactions of the customers, ordered by the transaction time. The most common interpretation of a transaction is that of a collection of the items a particular customer ordered (itemset). 

Sequential Patterns are very interesting in marketbasket analysis as they specify inter-transaction correlations, and e.g. discover `which items are frequently bought one after another.`

#### Product Recommendations

Recommendation engines are often built from rating data, provided by customers that were asked to vote for a certain product or service. From such data, the customer engagement for all products or services of a company can be derived. Items with the highest engagements are then used for a recommendation, hopefully filtered by those that are already in the cart of or have been purchased in recent transactions.

An alternative is to discover those items that were frequently bought together in the past. The respective relations between these products are derived from association rule mining and result in recommendations such as

> Customers who looked at or bought these items also looked at or bought those items.

The customer purchase behavior, and here the sequence of buyings, is an excellent indicator for the (hidden) customer's intent. Recommendations that also take the relations between sequences of buyings into account, therefore reflect customer behavior much better than other techniques.

The Sequence Mining Engine discovers the top sequential rules for item sequences that can be often found together and provides product recommendations from these rules. 

#### Web Usage Mining

In the context of web mining, especially web usage mining, companies need to understand what motivates their customers to purchase and how to influence the buying process to develop successful promotional activities.

Evaluating web sessions and the timely ordered sequences of page visits (within a certain time period), e.g. helps to understand similarities of click-streams much better than treating sessions as sets of page visits. As a results, visitors can be clustered or segmented not only by visited content, but also by their timely behavior and signatures.

---

#### Sequential Pattern Discovery using Equivalence Classes (SPADE)

SPADE is a fast and efficient algorithm to discover frequent sequential patterns from large databases. It utilizes combinatorial properties to decompose the mining task into smaller sub-tasks that can be independently solved in memory using efficient lattice search techniques, and using simple join operations.

We adapted the implementation of [Philippe Fournier-Viger](http://www.philippe-fournier-viger.com) and made the SPADE algorithm availaible for Apache Spark.

#### Top K Sequential Rules (TSR)

In 2011 [Philippe Fournier-Viger](http://www.philippe-fournier-viger.com) proposed a new algorithm to discover the [Top-K Sequential Rules](http://www.philippe-fournier-viger.com/spmf/TopSeqRules_sequential_rules_2.pdf) from a sequence database, similar to Top-K Association Rules from transaction databases.

We adapted Viger's original implementation and made his **Top-K Sequential Rules** algorithm available for Apache Spark.

---

#### Similarity of Sequential Patterns (S2MP)

Computing the similarity of sequential patterns is an important task to find regularities in sequences of data. This is the key to understand customer behavior, build profiles and signatures, and also to group similar customers by their temporally behavior.    

The similarity of sequential patterns may be used to cluster, (a) the content of sequence databases to retrieve more homogeneous datasets for sequential pattern mining, or (b) to group the respective mining results. The latter approach is used for customer segmentation based on similar engagement behavior.

For real-world applications, it is important to measure the similarity of more complex sequences, built from itemsets rather than single items. A customer's purchase behavior is an example of such a more complex sequence.

There exist already some similarity measures such as `Edit distance` (Levenshtein, 1996) and `LCS` (Longest Common Subsequence, 2002), but these methods do not take the content of the itemsets and their order and position in the sequences properly into account.

We therefore decided to implement the `S2MP` similarity measure proposed by [Saneifar et al](http://crpit.com/confpapers/CRPITV87Saneifar.pdf), which successfully overcomes the mentioned shortcomings.

From the similarity measure `sim(i,j)` of two sequences `i`and `j` it is straightforward to build the sequence engagement vector for sequence `i` with all other sequences. These vectors may then be used to build clusters with algorithms such as KMeans.

In market basket analysis or web usage mining, a sequence of purchase transactions or web sessions is directly associated with a certain customer or visitor. The clusters built from KMeans and S2MP may then be applied to group customers with similar buying or web usage behavior.

---

#### Clustering of Sequential Patterns (S-KMeans)

Clustering of sequential data or patterns becomes more and more relevant for business applications. `SKMeans` is a modified version of Apache Spark's KMeans algorithm and is optimized for clustering sequential patterns based on the `S2MP` similarity measure. 

