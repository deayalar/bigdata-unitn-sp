# bigdata-unitn-sp
Big Data project Unitn

Contributors:

David Ayala

Dario Fabiani


## Implications of Covid-19 on music preferences
The purpose of the project is to implement a big data system that collects, analyses the song’s popularity by looking at Spotify data (accessed with WebApi) in various countries, exploit implications of Covid-19 data to understand if making us staying at home influences music preferences. The output of our study is a set of visualizations made from the batch processing of data coming from different sources. These visualizations are backed on the Pleasure-Arousal-Model proposed by Russell in 1980. It expresses human emotions in a two-dimension space composed by arousal and pleasure. According to the findings of  Helmholz in 2017. These two dimensions can also be interpreted in terms of audio features like energy and valence. Details will be provided along this document.The code of the project can be found on GitLab, This is the repository

### Data sources and collection
This study will rely on the analysis of audio features. The audio features are metrics that describe different aspects like energy, valence, tempo or danceability of a song. Fortunately, the Spotify Web Api provides an endpoint to get this data based on the Spotify song IDs.
Furthermore, we also used the data regarding Covid-19 to improve our final analysis. We included the csv file that contains the total confirmed cases provided by Our World in Data because having the cumulative count of cases can reveal which countries have been impacted the most by the virus.
The next issue was to solve the question: “Which songs to use?”. Since in the project description it is asked to analyse songs popularity, we decided to use the “TOP 200 songs” from Spotify regional charts, which means the most popular ones. The site that contains this data for each country from the beginning of 2017 is spotifycharts.com. The WebAPI of Spotify does not provide this information.
The data provided by spotifycharts.com contain only a few characteristics such as title, artist, position in the chart and the spotify ID. The audio features are not provided here. Given this problem, we retrieved the needed audio features from the WebApi for each song using its ID. Once this step was accomplished, we would have all the data collected to start our analysis.

### Pleasure-Arousal-Model
The analysis part is based on the human emotional Pleasure-Arousal-Model created by James Russell. It arranges 28 human emotions according to the two dimension arousal and pleasure in circular order. In the case of music, which is also driven by emotions, the two-dimensional scale can be used to classify songs. In this case we used the audio feature energy, that corresponds to the human activation or arousal and the valence, that corresponds to the human pleasure or positiveness. Relying on this model makes the results  highly interpretable. 
Helmholz, Patrick & Siemon, Dominik & Robra-Bissantz, Susanne. (2017). Summer hot, Winter not! – Seasonal influences on context-based music recommendations.
The output of this solution are visualisations like the one shown above, but instead of having specific songs, we will have countries. This will allow us to visualize in which “mood” the country is. In further sections, we will describe how we found the values that characterise each country to be placed in the two-dimension space. In addition, we also used the data regarding Covid-19, to better visualize which countries were affected by the virus, how they moved within the two-dimensions, on which proportion compared to the others, and provide a useful plot. In the following part we delve into the project choices to actually implement what has been discussed above.

### Architectural Choices

![Architecture](https://github.com/deayalar/bigdata-unitn-sp/blob/master/source/architecture.jpg)

This solution is built on top of the AWS infrastructure because it avoids the installation of software that can be problematic in local machines and because we can ensure better communication between the different components. On the other hand, we decided to use Spark on top of Databricks to perform the analysis, this cluster also runs on AWS infrastructure.

1. SpotifyCharts.com Scrapper

From the site “spotifycharts.com” we wanted to get the “Weekly Top 200 songs” for each available country (not all the countries are listed). Since the site allows downloading a csv file, we were able to scrape the web site to get all the possible dates and call an endpoint to read the csv. Each file contains: Title, Artist, Position, the ID of the song and corresponds to one chart of a determined country. This component is a Python script that can run locally or in a basic AWS EC2 instance. It can run on multiple threads and has two main purposes. On one side, it sends the charts data to a DynamoDB instance and on the other hand it extracts the spotify IDs and sends them to an SQS queue (Producer). This works as an ETL of charts. 

2. DynamoDB

This is the main database of the system. The reason behind using DynamoDB is that the charts data can be represented as semi-structured data. So each row can be one chart with fields like date and country (this combination is the primary key), but the collection of songs is represented by a Json object that contains the Spotify ID among other fields. One advantage of DynamoDB is that we can interact with it using the client library boto3 for Python. It abstracts all the REST requests to perform CRUD operations on the database. In DynamoDB we have two main tables one for charts (semi-structured) and one for audio features (structured).

3. SQS

Mainly, we chose SQS on AWS to avoid local installation and configuration of Kafka that can be very complex. However the idea is the same, we want to communicate two different components of the system. On one side, the producer is the SpotifyCharts.com Scrapper (Component N°1) that builds individual messages. Every message is a list of unique Spotify IDs found on each batch of storage in the database. But the same ID can appear in more than one message. On the other side, the consumer of this queue is the Spotify Audio Features Collector (Component N°3); it reads every message, processes it and deletes the message after consumption.

4. Spotify Audio Features Collector

This component is a Python script that we created. Since we had the IDs of the songs, we used them to retrieve the audio features from the Spotify WebApi. This is the main goal of this component, it is also an ETL. It calls the Spotify Api to get the audio features in batches of 100 IDs according to the Spotify Api limitations and sends batches of the data to a specific DynamoDB table. The table is provided with the same schema of the Spotify response, first it is the ID of the songs, then all the audio features related to it.
This component is the consumer of the SQS queue to get the Spotify IDs that were in the charts. It also interacts with Redis that in the context of this solution acts as a cache system. Further details in the corresponding sections. 

5. Redis

Redis has a pivotal role in the architecture of this solution. It gives us a fast way to check and avoid repeating a request to the Web Api. In other words, it prevents the system to request the same ID multiple times. In this scenario we use Redis as a cache system, this is the main reason why we selected Redis. The component that interacts with it is the Spotify Audio Features Collector (Component N°3). It reads IDs from the SQS messages. For each ID, it checks if the ID exists in the Redis database, if not, it requests the audio features from Spotify, otherwise it does not do anything because it means that the audio features are already stored in the DynamoDB table.

6. Spark

Spark is the processing engine selected for this solution. The justification is that this data can be very large because it grows with the time. New charts means new songs, which means that distributed storage and processing is needed. In addition, joining the data between charts and audio features is not possible in a local machine using libraries like pandas because it will surpass the main memory. The Spark cluster will run on top of the Databricks platform, we chose it because it provides a comfortable way to interact with external data sources. To import the charts and audio features data from DynamoDB into Databricks tables we used the boto3 library. Another reason to choose Spark is that we can use the MLib library, it is useful because it will serve us to perform the analysis described in the next section.
