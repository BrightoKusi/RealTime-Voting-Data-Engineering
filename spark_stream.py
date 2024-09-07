import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, sum as _sum

if __name__ == "__main__":
    # Initialize Spark session with Kafka and PostgreSQL configurations
    spark = (SparkSession.builder
             .appName("RealtimeVotingStreamingAndAnalysis")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.4')
             .config('spark.sql.adaptive.enable', 'false')
             .getOrCreate())

    # Define the schema for vote data
    vote_schema = StructType([
        StructField('voter_id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', TimestampType(), True),
        StructField('voter_name', StringType(), True),
        StructField('party_affiliation', StringType(), True),
        StructField('biography', StringType(), True),
        StructField('campaign_platform', StringType(), True),
        StructField('photo_url', StringType(), True),
        StructField('candidate_name', StringType(), True),
        StructField('date_of_birth', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('registration_number', StringType(), True),
        StructField('address_street', StringType(), True),
        StructField('address_city', StringType(), True),
        StructField('address_state', StringType(), True),
        StructField('address_country', StringType(), True),
        StructField('address_postcode', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('picture', StringType(), True),
        StructField('registered_date', StringType(), True),
        StructField('vote', IntegerType(), True)
    ])

    # Read streaming data from Kafka 'votes_topic'
    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'votes_topic')
                .option('startingOffsets', 'earliest')
                .option('failOnDataLoss', 'false')
                .load())

    # Parse JSON data and apply schema
    votes_df = (votes_df.selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), vote_schema).alias('data'))
                .select('data.*')
                .withColumn('voting_time', col('voting_time').cast(TimestampType()))
                .withColumn('vote', col('vote').cast(IntegerType()))
                .withWatermark('voting_time', '1 minute'))

    # Aggregate total votes per candidate
    votes_per_candidate = (votes_df.groupBy('candidate_id', 'candidate_name', 'party_affiliation', 'photo_url')
                           .agg(_sum('vote').alias('total_votes')))

    # Aggregate voter turnout per location (state)
    turnout_per_location = (votes_df.groupBy('address_state')
                            .count()
                            .withColumnRenamed('count', 'total_voters'))

    # Write votes_per_candidate to Kafka topic 'aggregated_votes_per_candidate'
    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocation', "/home/bright/voting/VotingEngineering/checkpoints/checkpoint2")
                                    .outputMode('update')
                                    .start())

    # Write turnout_per_location to Kafka topic 'aggregated_turnout_per_location'
    turnout_per_location_to_kafka = (turnout_per_location.selectExpr('to_json(struct(*)) AS value')
                                     .writeStream
                                     .format('kafka')
                                     .option('kafka.bootstrap.servers', 'localhost:9092')
                                     .option('topic', 'aggregated_turnout_per_location')
                                     .option('checkpointLocation', "/home/bright/voting/VotingEngineering/checkpoints/checkpoint1")
                                     .outputMode('update')
                                     .start())

    # Wait for termination
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_per_location_to_kafka.awaitTermination()
