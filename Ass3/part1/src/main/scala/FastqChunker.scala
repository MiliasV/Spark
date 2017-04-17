import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io._
import java.io.File
import java.nio.file.{Paths, Files}

//import org.apache.hadoop.io.compress.GzipCodec
import java.util.zip._



object FastqChunker 
{

	
//function to write each partition in output/output#numberOfPartition.fq.gz
def writeToFile(iterator: Iterator[(Long, String)], outputFolder:String) = {
		//specify the output directory	
		val folder = new File(outputFolder)
		val firstRecord = iterator.next()
		//create the output directory
        folder.mkdir()
        //create the file where the data will be written
		var bw = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(folder  + "/output" + firstRecord._1 + ".fq.gz" )), "UTF-8")
		//write the value (read) of every element of the iterator to the file
		bw.write(firstRecord._2 + "\n")
		while(iterator.hasNext) {
			val value = iterator.next()
			if(iterator.hasNext){
				bw.write(value._2 +"\n")
			}
			else {
				bw.write(value._2)

			}
		}
   //close file
   bw.close();

	}

def main(args: Array[String]) 
{
	if (args.size < 3)
	{
		println("Not enough arguments!\nArg1 = number of parallel tasks = number of chunks\nArg2 = input folder\nArg3 = output folder")
		System.exit(1)
	}
	
	val prllTasks = args(0)
	val inputFolder = args(1)
	val outputFolder = args(2)
	
	if (!Files.exists(Paths.get(inputFolder)))
	{
		println("Input folder " + inputFolder + " doesn't exist!")
		System.exit(1)
	}
		 
	// Create output folder if it doesn't already exist	
	println("Number of parallel tasks = number of chunks = " + prllTasks + "\nInput folder = " + inputFolder + "\nOutput folder = " + outputFolder)
	
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	conf.setMaster("local[" + prllTasks + "]")
	conf.set("spark.cores.max", prllTasks)
	conf.set("spark.executor.instances", prllTasks)
	
	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	// Rest of the code goes here
				   //read file to RDD
	val fastq1 = sc.textFile(inputFolder + "/fastq1.fq")
				   //create an index for each line
				   .zipWithIndex
				   //transform RDD to (index, line)
				   .map{case(value, index) => (index, value)}

	//same logic with fastq1
	val fastq2 = sc.textFile(inputFolder + "/fastq2.fq")
				   .zipWithIndex
				   .map{case(value, index) => (index, value)}

	//Here I want to create groups of 4 lines. If I divide the index number by 4 then each 4 lines have a different
	//index. For example, for the first 4 lines (0, 1, 2, 3) => (0, 0, 0, 0)
	//					, the second 4 lines    (4, 5, 6, 7) => (1, 1, 1, 1) etc.
	val fq1_reads = fastq1.map{case(index, value) => ((index/4), value)}.groupByKey
	val fq2_reads = fastq2.map{case(index, value) => ((index/4), value)}.groupByKey

								 //Join the RDDs.
	val fastq1_fastq2 = fq1_reads.join(fq2_reads)
								 //Sort them by the index.
								 .sortByKey()
								 //Transform each record to (index, String with lines of reads separated by "\n").
								 .map{case(index, (read1, read2)) => (index,(read1.mkString("\n")+ "\n" +read2.mkString("\n") ))}
								// .map{case(index, (read1, read2)) => ((read1.mkString("\n")+ "\n" +read2.mkString("\n") ))}

								 	  //Repartion the RDD to the number of prllTasks.
								 	 //Here the partitions are also shuffled
	val fq_partitioned = fastq1_fastq2.repartition(prllTasks.toInt)
				  //write each partition to file (function: writeToFile)
	fq_partitioned.foreachPartition(x => writeToFile(x, outputFolder))


	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
