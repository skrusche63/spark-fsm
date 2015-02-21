![Elasticworks.](https://raw.githubusercontent.com/skrusche63/spark-fsm/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of predictive analytics to Elasticsearch.

## Reactive Series Analysis Engine

The Series Analysis Engine is one of the nine members of the open ensemble and is built to support sequential pattern mining with a new and redefined 
mining algorithm. The approach overcomes the well-known "threshold problem" and makes it a lot easier to directly leverage the resulting patterns and rules.

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

### Akka

Akka is a toolkit to build concurrent scalable applications, using the [Actor Model](http://en.wikipedia.org/wiki/Actor_model). Akka comes with a feature called *Akka Remoting*, which easily enables to setup 
a communication between software components in a peer-to-peer fashion.

Akka is leveraged in this software project to enable external software projects to interact with this Series Analysis engine. Besides external communication, Akka is also used to implement the internal 
interaction between the different functional building blocks of the engine:

* Administration
* Indexing & Tracking
* Training
* Retrieval 

---

### Data Sources

The Reactive Association Analysis Engine supports a rapidly increasing list of applicable data sources. Below is a list of data sources that are already supported:

* Cassandra,
* Elasticsearch,
* HBase,
* MongoDB,
* Parquent,

and JDBC database.

