from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS
import json
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize SparkSession
spark = SparkSession.builder.appName("MovieReviews").config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g").getOrCreate()

# Load dataset files into DataFrames
movies_df = spark.read.csv("ml-25m/movies.csv", header=True)
ratings_df = spark.read.csv("ml-25m/ratings.csv", header=True)
tags_df = spark.read.csv("ml-25m/tags.csv", header=True)

# Join DataFrames to get the required information
movie_reviews_df = ratings_df.join(movies_df, on="movieId").join(tags_df, on=["userId", "movieId"])
# movie_reviews_df.show(5)

# Filter reviews based on ratings to separate positive and negative reviews
positive_reviews_df = movie_reviews_df.filter(col("rating") >= 4)
negative_reviews_df = movie_reviews_df.filter(col("rating") < 4)


# Calculate the ratio of positive to negative reviews for each movie
positive_reviews_count = positive_reviews_df.groupBy("movieId").count().withColumnRenamed("count", "positive_count")
negative_reviews_count = negative_reviews_df.groupBy("movieId").count().withColumnRenamed("count", "negative_count")
reviews_ratio_df = positive_reviews_count.join(negative_reviews_count, on="movieId", how="outer").fillna(0)
movie_reviews_info = movies_df.join(reviews_ratio_df, on="movieId", how="left")
# movie_reviews_info.show(5)


# Convert userId and movieId columns to numeric data type
ratings_df = ratings_df.withColumn("userId", ratings_df["userId"].cast(IntegerType()))
ratings_df = ratings_df.withColumn("movieId", ratings_df["movieId"].cast(IntegerType()))
ratings_df = ratings_df.withColumn("rating", ratings_df["rating"].cast(FloatType()))

# Train ALS model for movie recommendations
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative=True, coldStartStrategy="drop")
model = als.fit(ratings_df)

def movie_info_provider (title):
    movie_info = {}
    # Analyze a movie
    # title = input("Enter the title of the movie: ")

    selected_movie = movie_reviews_info.filter(col("title") == title).first()

    if selected_movie:
        movie_id = selected_movie["movieId"]
        positive_count = selected_movie["positive_count"]
        negative_count = selected_movie["negative_count"]
        total_reviews = positive_count + negative_count
        positive_percentage = (positive_count / total_reviews) * 100
        negative_percentage = (negative_count / total_reviews) * 100

        print("Movie Information:")
        print(f"Title: {selected_movie['title']}")
        print(f"Genre: {selected_movie['genres']}")
        print(f"Positive Reviews: {positive_count} ({positive_percentage:.2f}%)")
        print(f"Negative Reviews: {negative_count} ({negative_percentage:.2f}%)")
        movie_info['title'] = selected_movie['title']
        movie_info['genres'] = selected_movie['genres']
        movie_info['positive_count'] = positive_count
        movie_info['positive_percentage'] = positive_percentage
        movie_info['negative_count'] = negative_count
        movie_info['negative_percentage'] = negative_percentage

        # Show three tags of the movie
        movie_tags = tags_df.filter(col("movieId") == movie_id).select("tag").limit(3)
        tag_list = list()
        if movie_tags.count() > 0:
            print("Tags:")
            for tag in movie_tags.collect():
                print(tag["tag"])
                tag_list.append(tag["tag"])

        movie_info['tags'] = tag_list

        print('----------movie_info----------')
        print(movie_info)
        # dictionary를 JSON 문자열로 변환
        # json_string = json.dumps(movie_info)
        # print(json_string)
        return movie_info

    else:
        print("Movie not found.")

def genre_top_5_provider(genre):
    top_list = list()

    genre_movies = movie_reviews_info.filter(col("genres").contains(genre))
    sorted_movies = genre_movies.sort(col("positive_count").desc()).limit(5)

    print(f"Top 5 movies in the {genre} genre:")
    for movie in sorted_movies.collect():
        movie_info = {}
        movie_id = movie["movieId"]
        positive_count = movie["positive_count"]
        negative_count = movie["negative_count"]
        total_reviews = positive_count + negative_count
        positive_percentage = (positive_count / total_reviews) * 100
        negative_percentage = (negative_count / total_reviews) * 100

        print("Movie Information:")
        print(f"Title: {movie['title']}")
        print(f"Genre: {movie['genres']}")
        print(f"Positive Reviews: {positive_count} ({positive_percentage:.2f}%)")
        print(f"Negative Reviews: {negative_count} ({negative_percentage:.2f}%)")
        movie_info['title'] = movie['title']
        movie_info['genres'] = movie['genres']
        movie_info['positive_count'] = movie['positive_count']
        movie_info['positive_percentage'] = positive_percentage
        movie_info['negative_count'] = movie['negative_count']
        movie_info['negative_percentage'] = negative_percentage


        movie_tags = tags_df.filter(col("movieId") == movie_id).select("tag").limit(3)
        tag_list = list()
        if movie_tags.count() > 0:
            print("Tags:")
            for tag in movie_tags.collect():
                print(tag["tag"])
                tag_list.append(tag["tag"])

        movie_info['tags'] = tag_list
        top_list.append(movie_info)

    return top_list

def user_based_recommendation_provider(user_id):
    recommendation_list = list()
    movie_info = {}

    user_ratings = ratings_df.filter(col("userId") == user_id)
    average_rating = user_ratings.agg({"rating": "avg"}).first()[0]

    if average_rating >= 4:
        rating_type = "high"
    else:
        rating_type = "low"

    print(f"User {user_id} tends to give {rating_type} ratings.")


    # Step 1: Create a new DataFrame containing only the movies seen by a specific user_id
    watched_movies = ratings_df.filter(col("userId") == user_id).join(
        movie_reviews_info.select("movieId", "title", "genres"), on="movieId", how="left")
    # watched_movies.show(5)

    # Step 2: Join watched_movies to the existing movie data frame to identify movies that user_id has not seen
    unseen_movies = movie_reviews_info.join(watched_movies.select("movieId"), on="movieId", how="left_anti")
    # unseen_movies.show(5)

    # Step 3: Create a new DataFrame that contains movies similar to the genre of watched movies among movies that user_id has not seen
    genre_list = [watched_movies.select("genres").distinct().collect()[0][0]]
    similar_movies = unseen_movies.filter(col("genres").isin(genre_list))

    # Step 4: Use the ALS model to predict recommended ratings
    similar_movies = similar_movies.withColumn("userId", lit(user_id))
    similar_movies = similar_movies.withColumn("movieId", similar_movies["movieId"].cast(IntegerType()))
    user_recommendations = model.transform(similar_movies)

    # Step 5: Get the top 5 recommendations in order of highest ratings
    top_recommendations = user_recommendations.orderBy(col("prediction").desc()).limit(5)

    # Print the recommended movies
    print(f"Recommended movies for user {user_id}:")
    for movie in top_recommendations.collect():
        movie_info = {}
        print("Movie Information:")
        print(f"Title: {movie['title']}")
        print(f"Genre: {movie['genres']}")
        movie_info['title'] = movie['title']
        movie_info['genres'] = movie['genres']
        movie_tags = tags_df.filter(col("movieId") == movie["movieId"]).select("tag").limit(3)
        tag_list = list()
        if movie_tags.count() > 0:
            print("Tags:")
            for tag in movie_tags.collect():
                print(tag["tag"])
                tag_list.append(tag['tag'])

        movie_info['tags'] = tag_list
        print(f"Predicted Rating: {movie['prediction']}")
        print("--------------------")

    # Measure the RMSE and MAE values
    predictions = model.transform(ratings_df)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
    mae = evaluator.evaluate(predictions)

    print(f"RMSE: {rmse}")
    print(f"MAE: {mae}")

    recommendation_list.append(movie_info)

# user_based_recommendation_provider(1)

