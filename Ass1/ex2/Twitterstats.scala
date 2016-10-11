import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{  
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount)
	def write2Log(a: Array[(String, Long, Long, String, Long, Long)])
	{
		if (firstTime)
		{
			bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val langCode = a(i)._1
				val lang = getLangName(langCode)
				val totalRetweetsInThatLang = a(i)._2
				val id = a(i)._3
				val textStr = a(i)._4.replaceAll("\\r|\\n", " ")
				val maxRetweetCount = a(i)._5
				val minRetweetCount = a(i)._6
				val retweetCount = maxRetweetCount - minRetweetCount + 1
				
				bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
					maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String]) 
	{
		// Configure Twitter credentials
		//I removed my credentials for security reasons so to run this exercise you have
		//to configure it with your Twitter credentials
		val apiKey = ""
		val apiSecret = ""
		val accessToken = ""
		val accessTokenSecret = ""
		
		Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
		
		val ssc = new StreamingContext(new SparkConf(), Seconds(5))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		// Add your code here

		val statuses = tweets.map(status => status.getText())
    	
    	//filter the tweets so that we end up only with the retweets
    	val retweets = tweets.filter(tweet => (tweet.isRetweet()))

    								//we define a window of 60 seconds
    	val retweetMinMax = retweets.window(Seconds(60))
    								//we transform each RDD of DStream to 
    								// (id of the original tweet, 
    								//(retweet count of the original tweet (we will use it to compute the max retweet count per window),
    								//retweet count of the original tweet (we will use it to compute the min retweet count per window)
    								// text of the original tweet) )
    						  		.map(status => (status.getRetweetedStatus.getId(),
    						  			( status.getRetweetedStatus.getRetweetCount().toString, 
    						   			 status.getRetweetedStatus.getRetweetCount().toString, 
    						   			 status.getRetweetedStatus.getText())))
    						  		// We reduce by key and we end up with
    						  		//(id of the original tweet, (max retweet count, min retweet count, text of original tweet))
    						   		.reduceByKey( (a,b) => (Math.max(a._1.toInt,b._1.toInt).toString, 
    						   								Math.min(a._2.toInt,b._2.toInt).toString, a._3 ))

							//we transform the retweets to (id of original tweet, (language of the original tweet))    						   	
    	val idLangCodeText = retweets.map(status => (status.getRetweetedStatus.getId(),
    										  		(getLang(status.getRetweetedStatus.getText()))))

    	
    									  // we combine the previous results 
		val retweetCount = idLangCodeText.join(retweetMinMax)
								   // we transform each RDD to (langCode, (id of original tweet, 
								   //	max(retweet count), min(retweet count), max - min + 1 , text of original tweet))
    							  .map(l =>(l._2._1, ( (l._1).toString, (l._2._2._1).toString, (l._2._2._2).toString, 
    							  			((l._2._2._1).toInt - (l._2._2._2).toInt + 1).toString, l._2._2._3	 )))


												   // we group retweetCount by langCode
    	val totalRetweetsInThatLang = retweetCount.transform((rdd => rdd.groupBy(_._1)
 												   //for each langCode we sum the retweet count in that window
 												   //and we have (langCode, retweetCount)
    											  .mapValues(_.map(_._2._4.toInt).sum)))
    										
							 	   			    
    								 //we combine retweetCount & totalRetweetsInThatLang
		val outDStream = retweetCount.join(totalRetweetsInThatLang)
									//we transform each RDD to 
									// ((totalRetweetsInThatLang, langCode, retweetCount), id of original tweet, text )
							 		 .map( l => ((l._2._2, l._1, l._2._1._4 ), (l._2._1._1, l._2._1._5, l._2._1._2, l._2._1._3)))
							  		 //we sort by the key in a descending order which means 
							  		 //we sort firstly by TotalRetweetsInThatLang
							  		 //if some languages have the same TotalRetweetsInThatLang we then sort
							  		 // by tha langCode, and after that we sort by retweetCount for each language
							  		 .transform((rdd => rdd.sortBy(_._1,false)))
							  		 //finaly we transform each rdd to the desirable output format
							  		 .map(l => (l._1._2, l._1._1.toLong, l._2._1.toLong, l._2._2, l._2._3.toLong, l._2._4.toLong))
							 
		
		// If elements of RDDs of outDStream aren't of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount),
		//	then you must change the write2Log function accordingly.
		outDStream.foreachRDD(rdd => write2Log(rdd.collect))
	
		new java.io.File("cpdir").mkdirs
		ssc.checkpoint("cpdir")
		ssc.start()
		ssc.awaitTermination()
	}
}

