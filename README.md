# FastTweetSearch

*High level Architecture :*
![Architecture](./assets/architecture.png)


## PROJECT GOALS

- **Summarize the Twitter Dataset**: Generate a detailed report covering the total number of users, tweets, and other key data facets like hashtags, mentions, and URLs.

- **Efficient Data Storage**: Store tweet information across two datastores (one relational for user data and one non-relational for tweets) for optimized access and querying, incorporating indexing strategies.

- **Implement a Cache System**: Design and implement a caching mechanism(from scratch) for frequently accessed data to improve retrieval times, with strategies for eviction, data staleness handling, and periodic checkpointing.

- **Develop a Search Application**: Create a versatile search application capable of querying by string, hashtag, user, and time range, including drill-down features for detailed tweet metadata and providing top-level metrics.

- **Performance Evaluation**: Conduct testing with a set of representative queries to measure performance metrics both with and without cache, aiming to demonstrate the effectiveness of the caching strategy and the efficiency of data retrieval.