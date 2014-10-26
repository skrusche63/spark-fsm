![Elasticworks.](https://raw.githubusercontent.com/skrusche63/spark-fsm/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of predictive analytics to Elasticsearch.

## Reactive Series Analysis Engine

![Series Analysis Engine Overview](https://raw.githubusercontent.com/skrusche63/spark-fsm/master/images/series-analysis-overview.png)

The Series Analysis Engines is one of the nine members of the open ensemble and is built to support sequential pattern mining with a new and redefined 
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

Akka is a toolkit to build concurrent scalable applications, using the [Actor Model](http://en.wikipedia.org/wiki/Actor_model). Akka comes with a feature called *Akka Remoting*, which easily enables to setup a communication between software components in a peer-to-peer fashion.

Akka and Akka Remoting are an appropriate means to establish a communication between prior independent software components - easy and fast.

---

### Spark

From the [Apache Spark](https://spark.apache.org/) website:

> Apache Spark is a fast and general engine for large-scale data processing and is up to 100x faster than Hadoop MR in memory.

The increasing number of associated projects, such as [Spark SQL](https://spark.apache.org/sql/) and [Spark Streaming](https://spark.apache.org/streaming/), enables Spark to become the future  Unified Data Insight Platform. With this perspective in mind, we have integrated recently published Association Rule algorithms with Spark. This allows for a seamless usage of association rule mining either with batch or streaming data sources.

---

### Data Sources

The Reactive Series Analysis Engine supports a rapidly increasing list of applicable data sources. Below is a list of data sources that are already supported or 
will be supported in the near future:

#### Elasticsearch

[Elasticsearch](http://www.elasticsearch.org) is a flexible and powerful distributed real-time search and analytics engine. Besides linguistic and semantic 
enrichment, for data in a search index there is an increasing demand to apply analytics, knowledge discovery & data mining, and even predictive analytics 
to gain deeper insights into the data and further increase their business value.

A step towards analytics is the recently introduced combination with [Logstash](http://logstash.net/) to easily store logs and other time based event data 
from any system in a single place.

The Series Analysis Engine comes with a connector to Elasticsearch and thus brings knowledge discovery and data mining to the world of indexed data. The use 
cases are endless. 

#### Piwik Analytics

[Piwik Analytics](http://piwik.org) is the leading and widely used open source web analytics platform, and is an excellent starting point to move into the world 
of dynamic catalogs, product recommendations, purchase predictions and more.

#### Pimcore (coming soon)

[Pimcore](http://pimcore.org) is an open source multi-channel experience and engagement management platform and contains a variety of integrated applications such 
as Digital Asset Management, Ecommerce Framework, Marketing & Campaign Management, Multi-Channel & Web-to-Print, Product Information Management, Targeting & Personalization and
Web Content Management.

#### Relational Databases

Elasticsearch is one of the connectors actually supported. As many ecommerce sites and analytics platforms work with JDBC databases, the Series Analysis Engine 
also comes with a JDBC connector.

---

### Data Sinks

#### Elasticsearch

The Series Analysis Engine writes discovered patterns and rules to an Elasticsearch index. This ensures that these rules e.g. may directly be used for product recommendations delivered with 
appropriate product search results.

#### Redis

[Redis](http://redis.io) is open source and an advanced key-value cache and store, often referred to as a distributed data structure server. The Series Analysis Engine writes discovered patterns 
and rules to a Redis instance as a multi-purpose serving layer for software enrichments that are not equipped with Elasticsearch.

---

### Technology

* Akka
* Akka Remoting
* Elasticsearch
* Redis
* Spark
* Spark Streaming
* Spray

