const AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis();
const twitter = require('twitter');
const STREAM_NAME = 'doe-kinesis-stream';
const uuidv1 = require('uuid/v1');


var client = new twitter({
  consumer_key: process.env.TWITTER_CONSUMER_KEY,
  consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
  access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
  access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
});


exports.handler = async (event) => {
     
    console.log(event);
    console.log('Poll Twitter for search term: ', event.searchText);

	return pollTwitter(event.searchText).then(function(tweets) {

        console.log("Stream tweets: ", JSON.stringify(tweets, null, 2));
        console.log('tweets.statuses.length: ', tweets.statuses.length);

		if (tweets != null && tweets.statuses != null) {
			tweets.statuses.forEach((tweet) => { 
				console.log('Stream tweet: ', JSON.stringify(tweet, null, 2));
				return streamTweets(JSON.stringify(tweet, null, 2));
			});
		}

        return streamTweets(tweets);

    }, function(err) {
        console.log(err);
    });
};

function pollTwitter(searchText) {
	return new Promise(function(resolve, reject) {
		client.get('search/tweets', {q: searchText}, function(error, tweets, response) {
			console.log(tweets);
			resolve(tweets);
		});
    });
};

function streamTweets (record) {

	var paramsOld = {
		DeliveryStreamName: STREAM_NAME,
		Record: { 
			Data: JSON.stringify(record, null, 2)
		}
	};

	var params = {
		Data: record,
		PartitionKey: uuidv1(),
		StreamName: 'STRING_VALUE',
	};


	return new Promise(function(resolve, reject) {
		kinesis.putRecord(params, function(err, data) {
            if (err) {
                reject(err);
            } else {
                resolve(record);
            }
		});
    });
}
