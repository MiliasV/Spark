import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils


object AnalyzeTwitters
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def main(args: Array[String]) 
	{
		val inputFile = args(0)
		val conf = new SparkConf().setAppName("AnalyzeTwitters")
		val sc = new SparkContext(conf)

		// Comment these two lines if you want more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here

		//drop(1) so that we don't take the headers
		val file=sc.textFile(inputFile).mapPartitions(_.drop(1))	
		
							    //split the file per "," (this means that the retweet text has been splitted too)
		val retweetCount = file.map(l =>l.split(","))
						   //map to ({IDOfTweet}, (Language, Lang-code, TotalRetweetsInThatLang, 
			   			  //           MaxRetweetCount, MinRetweetCount, RetweetCount, Text ))
						  // for taking back the text we drop the first elements of rdd and we make a 
						  //string with the rest where we add a "," for each space
						 .map(l=> ( l(4), (l(1), l(2), l(3), l(5), l(6), l(7), l.drop(8).mkString(","))))
						 //find the maximum of the MaxRetweetCount per tweet (max)
			   			 //				and the minimum of the MinRetweetCount per Tweet (min)
						 .reduceByKey((a,b) => ( a._1, a._2, a._3, 
							 						Math.max(a._4.toInt,b._4.toInt).toString, 
							 						Math.min(a._5.toInt,b._5.toInt).toString,
							 						a._6, a._7))
						 //transform to ({Language}, (Lang-code, IDOfTweet, (max - min + 1), text))
						 .map(l => (l._2._1 ,(l._2._2, l._1, (l._2._4.toInt - l._2._5.toInt + 1).toString, l._2._7))).cache()

										 // Group by language and 
		val totalSumByLang = retweetCount.groupBy(_._1)
								   //sum the value: retweet count
							 	   .mapValues(_.map(_._2._3.toInt).sum)
							  							
						
								 //join retweetCount + totalSumByLang
		val outRDD = retweetCount.join(totalSumByLang)
							//map to ({TotalRetweetsInThatLang, Language, RetweetCount}, 
			   			    //		     (Lang-code, IDOfTweet, Text))
						    .map(l => ((l._2._2.toInt, l._1, l._2._1._3.toInt), (l._2._1._1, l._2._1._2, l._2._1._4)))
							//leave entries with RetweetCount< 2
							.filter(l => (l._1._3>1))
							//sort by the key which means sort first by TotalRetweetsInThatLang,
			   				//then by Language & finally by RetweetCount
							.sortBy(_._1,false)
							//finaly we transform to the desirable output format
							.map(l => l._1._2 + "," + l._2._1 + "," + l._1._1 +  "," + l._2._2 + "," +l._1._3 + "," + l._2._3 )
		// Write output
		
		val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output3.txt"), "UTF-8"))

		//val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))
		bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")
		outRDD.collect.foreach(x => bw.write(x + "\n"))
		bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et /60, et % 60))
	}
}

