package com.kafka.elasticsearch.constants;

public interface CommonConstants {
	
	String BOOTSTRAP_SERVERS = "localhost:9092";
	String GROUP_ID = "kafka-demo-elastic-search";

	// Topics
	String TWITTER_TWEETS_TOPIC = "twitter_tweets";
	
	
	// Twitter related config
	String TWITTER_CONSUMER_KEY = "u0HhTeZKJHL0A9Oo74BlmiOfX";
	String TWITTER_CONSUMER_SECRET = "i8LwIPX8OyE7R0mTT4QOS9kjJFE4vozWLCF5z3pyei8FtETOs1";
	String TWITTER_ACCESS_TOKEN = "1403629042188705794-yeKAVwwTjGi9OX9VLUja9yPugfy0mw";
	String TWITTER_ACCESS_SECRET = "nJAVTx8LDwb9OYohpDxuxCo4rlDW0Bw53SBzw7CYSP1yr";
	//List<String> TWITTER_TERMS = Lists.newArrayList("bitcoin");

}
