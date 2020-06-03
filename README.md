# **Community Detection In Graphs**

## **Objective:**

In this project, we will implement the Girvan-Newman algorithm to detect communities in graphs where each community is a set of users who have a similar business taste, using the Yelp dataset. We will identify these communities using map-reduce, Spark RDD and standard python libraries.

## **Environment Setup:**
* Python Version = 3.6
* Spark Version = 2.3.3

## **Dataset:**

The dataset sample_data.csv is a sub-dataset which has been generated from the Yelp review dataset. Each line in this dataset contains a user_id and business_id.

## **Approach:**
(Reference: 'Mining of Massive Datasets' by Jure Leskovec, Anand Rajaraman and Jeffrey D. Ullman)

## **Graph Construction:**
We first construct the social network graph, where each node represents a user. An edge exists between two nodes if the number of times that the two users review the same business is greater than or equivalent to the filter threshold. For example, suppose user1 reviews [business1, business2, business3] and user2 reviews [business2, business3, business4, business5]. If the threshold is 2, then there exists an edge between user1 and user2. If a user node has no edge, then we do not include that node in the graph.

## **Betweenness Calculation:**
In this part, we calculate the betweenness of each edge in the original graph and save the result in a txt file. The betweenness of an edge (a, b) is defined as the number of pairs of nodes x and y such that the edge (a ,b) lies on the shortest path between x and y. We use the Girvan-Newman Algorithm to calculate the number of shortest paths going through each edge. In this algorithm, we visit each node X once and compute the number of shortest paths from X to each of the other nodes that go through each of the edges as shown in the below steps:

* First, perform a breadth-first search (BFS) of the graph, starting at node X.
* Next, label each node by the number of shortest paths that reach it from the root. Start by labelling the root 1. Then, from the top down, label each node Y by the sum of the labels of its parents.
* Calculate for each edge e, the sum over all nodes Y (of the fraction) of the shortest paths from the root X to Y that go through edge e.
* To complete the betweenness calculation, we have to repeat this calculation for every node as the root and sum the contributions.Finally, we must divide by 2 to get the true betweenness, since every shortest path will be discovered twice, once for each of its endpoints.
