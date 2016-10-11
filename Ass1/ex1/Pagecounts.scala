import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils
import scala.io.Source

object Pagecounts 
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def main(args: Array[String]) 
	{
		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("Pagecounts")
		val sc = new SparkContext(conf)
		
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here
		val file=sc.textFile(inputFile)
		// Take the text before dot
		val beforeDot = (x:String) => StringUtils.substringBefore(x,".")
	
		val outRDD = file.map(l => l.split(" ")) 
						 //remove the lines that have the same string as a lang-code and as a Page Title
						 .filter(l => beforeDot(l(0))!=l(1)) 

						 //format each element of RDD as (langcode, (Views, Page, Views))													
						 .map(l => ( beforeDot(l(0)), ( l(2).toInt, l(1), l(2).toInt )))

						 // with reduceByKey sum the Views for each langCode (TotalViewsInThatLang)
						 // and keep the Page with the most Views (MostVisitedPageInThatLang,ViewsOfThatPage)
						 //(langcode,( TotalViewsInThatLang, MostVisitedPAgeInThatLang, ViewsOfThatPage))
						 .reduceByKey( (a,b) => ( a._1 + b._1, if (a._3 > b._3) a._2 else b._2, if (a._3>b._3) a._3 else b._3)) 

						 //sort by TotalViewsInThatLang
						 .sortBy(_._2._1.toInt,false)

						 //Format each element of the RDD as the final output String
						 //Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage
						 .map(l => getLangName(l._1) + "," + l._1 + "," + l._2._1  + "," + l._2._2  + "," + l._2._3 )
		
					 	 	
		// Write output in a file
		//val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output.txt"), "UTF-8"))

		// Write output

		val bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"))

		bw.write("Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
		outRDD.collect.foreach(x => bw.write(x + "\n"))
		bw.close
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

