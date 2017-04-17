/* 
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import sys.process._
import scala.sys.process.Process

import java.io._
import java.nio.file.{Paths, Files}
import collection.mutable.ArrayBuffer
import org.apache.commons.io.FileUtils

import org.apache.spark.HashPartitioner




import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator

import htsjdk.samtools._

import org.apache.spark.TaskContext

object DNASeqAnalyzer 
{
final val MemString = "-Xmx5120m" 
final val RefFileName = "ucsc.hg19.fasta"
final val SnpFileName = "dbsnp_138.hg19.vcf"
final val ExomeFileName = "gcat_set_025.bed"
//////////////////////////////////////////////////////////////////////////////

def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}

def bwaRun (x: String, config: Configuration) : 
	Array[(Int, SAMRecord)] = 
{
	
	//Definition of the folders that i am going to use
	val refFolder = config.getRefFolder
	val tmpFolder = config.getTmpFolder
	val numOfThreads = config.getNumThreads
	val outFileName = config.getTmpFolder + x.split("/").last + ".sam"
	val tools = config.getToolsFolder
	val outputFolder = config.getOutputFolder
	//The folders log/bwa should be already inside the outputFolder
	val logFolder = outputFolder + "log/bwa/" + x.split("/").last.split("\\.")(0) + "_log.txt"

	//Folder used to log the commands
	val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFolder), "UTF-8"))
 
	// Create the command string (bwa mem...)and then execute it using the Scala's process package. More help about 
	//	Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package. 

	// bwa mem refFolder/RefFileName -p -t numOfThreads fastqChunk > outFileName

	//Create the command
	var cmd_Seq = Seq(s"${tools}bwa","mem",refFolder + RefFileName,"-p","-t",numOfThreads, x)

	//Write the command to the log file and then execute it
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq #> new File(outFileName) !


	val bwaKeyValues = new BWAKeyValues(outFileName)
	bwaKeyValues.parseSam()
	val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()
	//return null

	// Delete the temporary files
	FileUtils.deleteQuietly(new File(outFileName))

	writer.close()

	return kvPairs // Replace this with return kvPairs
}
	 
def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], config: Configuration) : ChromosomeRange = 
{

	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	val outHeader = header.clone()
	outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
	val factory = new SAMFileWriterFactory();
	val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));
	
	val r = new ChromosomeRange()
	val input = new SAMRecordIterator(samRecordsSorted, header, r)
	while(input.hasNext()) 
	{
		val sam = input.next()
		writer.addAlignment(sam);
	}
	writer.close();
	
	return r
}

def variantCall (chrRegion: Int, samRecordsSorted: Array[SAMRecord], config: Configuration) : 
	Array[(Integer, (Integer, String))] = 
{	
	//Definition of the folders that i am going to use
	val tmpFolder = config.getTmpFolder
	val toolsFolder = config.getToolsFolder
	val refFolder = config.getRefFolder
	val numOfThreads = config.getNumThreads
	val outputFolder = config.getOutputFolder
	//The folders log/vc should be already there
	val logFolder = outputFolder + "log/vc/"

	//Folder used to log the commands
	val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFolder + "output" + chrRegion + "_log.txt"), "UTF-8"))

	// Following is shown how each tool is called. Replace the X in regionX with the chromosome region number (chrRegion). 
	// 	You would have to create the command strings (for running jar files) and then execute them using the Scala's process package. More 
	// 	help about Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.
	//	Note that MemString here is -Xmx6144m, and already defined as a constant variable above, and so are reference files' names.

	// SAM records should be sorted by this point
	//val chrRange = writeToBAM(tmpFolder/regionX-p1.bam, samRecordsSorted, config)

	//Definition of the names of the files
	val regionP1 = tmpFolder + "region" + chrRegion + "-p1.bam"
	val regionP2 = tmpFolder + "region" + chrRegion + "-p2.bam"
	val regionP3 = tmpFolder + "region" + chrRegion + "-p3.bam"
	val regionX = tmpFolder + "region" + chrRegion + ".bam"

	val chrRange = writeToBAM(regionP1, samRecordsSorted, config)
	
	//Now I am executing and logging all the commands which were given. Each time I create a Seq that includes
	//the command, I then write this command to the log file, and finally i am executing it.

	// Picard preprocessing
		//	java MemString -jar toolsFolder/CleanSam.jar INPUT=tmpFolder/regionX-p1.bam OUTPUT=tmpFolder/regionX-p2.bam
		
	var cmd_Seq = Seq("java", MemString, 
				  "-jar", toolsFolder + "CleanSam.jar",
				  "INPUT=" + regionP1, 
				  "OUTPUT=" + regionP2)
	
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	val metricsFile = tmpFolder + "region" + chrRegion + "-p3-metrics.txt"

	//	java MemString -jar toolsFolder/MarkDuplicates.jar INPUT=tmpFolder/regionX-p2.bam OUTPUT=tmpFolder/regionX-p3.bam
	
	cmd_Seq = Seq("java", MemString,
			  "-jar", toolsFolder + "MarkDuplicates.jar", 
			  "INPUT=" + regionP2, 
			  "OUTPUT=" + regionP3,
			  "METRICS_FILE=" + metricsFile)
 	
 	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	//		METRICS_FILE=tmpFolder/regionX-p3-metrics.txt
	//	java MemString -jar toolsFolder/AddOrReplaceReadGroups.jar INPUT=tmpFolder/regionX-p3.bam OUTPUT=tmpFolder/regionX.bam

	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "AddOrReplaceReadGroups.jar", 
			  "INPUT=" + regionP3, 
			  "OUTPUT=" + regionX, 
			  "RGID=GROUP1", "RGLB=LIB1", "RGPL=ILLUMINA", "RGPU=UNIT1", "RGSM=SAMPLE1")
	
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
		cmd_Seq!

	//		RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNIT1 RGSM=SAMPLE1

	// 	java MemString -jar toolsFolder/BuildBamIndex.jar INPUT=tmpFolder/regionX.bam

	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "BuildBamIndex.jar", 
			  "INPUT=" + regionX)
	
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	//	delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt

	FileUtils.deleteQuietly(new File(regionP1))
	FileUtils.deleteQuietly(new File(regionP2))
	FileUtils.deleteQuietly(new File(regionP3))
	FileUtils.deleteQuietly(new File(metricsFile))


	// Make region file 
	//	val tmpBed = new File(tmpFolder/tmpX.bed)
	val tmpBedString = tmpFolder + "tmp" + chrRegion + ".bed"
	val tmpBed = new File(tmpBedString)
	val regIntervals = tmpFolder + "region" + chrRegion + ".intervals"
	val bedX = tmpFolder + "bed" + chrRegion + ".bed"
	val regionX2 = tmpFolder +  "region" + chrRegion +"-2.bam"


	chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())

	//	toolsFolder/bedtools intersect -a refFolder/ExomeFileName -b tmpFolder/tmpX.bed -header > tmpFolder/bedX.bed
	cmd_Seq = Seq(toolsFolder + "bedtools", "intersect", 
			 "-a", refFolder + ExomeFileName,
			 "-b", tmpBedString, "-header")
	
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq #> new File(bedX)!


	//	delete tmpFolder/tmpX.bed
	FileUtils.deleteQuietly(new File(tmpBedString))
	
	// Indel Realignment 
	//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T RealignerTargetCreator -nt numOfThreads -R refFolder/RefFileName 
	//		-I tmpFolder/regionX.bam -o tmpFolder/regionX.intervals -L tmpFolder/bedX.bed
	
	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "GenomeAnalysisTK.jar",
			  "-T", "RealignerTargetCreator",
			  "-nt", numOfThreads, 
			  "-R", refFolder + RefFileName, 
			  "-I", regionX, 
			  "-o", regIntervals, 
			  "-L", bedX)

	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T IndelRealigner -R refFolder/RefFileName -I tmpFolder/regionX.bam 
	//		-targetIntervals tmpFolder/regionX.intervals -o tmpFolder/regionX-2.bam -L tmpFolder/bedX.bed
	
	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "GenomeAnalysisTK.jar", 
			  "-T", "IndelRealigner", 
			  "-R",refFolder + RefFileName, 
			  "-I", regionX, 
			  "-targetIntervals", regIntervals, 
			  "-o", regionX2, 
			  "-L", bedX )

	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	//	delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
	

	FileUtils.deleteQuietly(new File(regionX))
	FileUtils.deleteQuietly(new File(regIntervals))
	FileUtils.deleteQuietly(new File(tmpFolder + "region" + chrRegion +".bai"))

	// Base quality recalibration 
	val regionTable = tmpFolder + "region" + chrRegion + ".table"
	val regionX3 = tmpFolder + "region" + chrRegion +"-3.bam"


	//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T BaseRecalibrator -nct numOfThreads -R refFolder/RefFileName -I 
	//		tmpFolder/regionX-2.bam -o tmpFolder/regionX.table -L tmpFolder/bedX.bed --disable_auto_index_creation_and_locking_when_reading_rods 
	//		-knownSites refFolder/SnpFileName
	
	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "GenomeAnalysisTK.jar", 
			  "-T", "BaseRecalibrator", 
			  "-nct", numOfThreads, 
			  "-R", refFolder + RefFileName, 
			  "-I", regionX2, "-o", regionTable, 
			  "-L", bedX, 
			  "-disable_auto_index_creation_and_locking_when_reading_rods",
		 	  "-knownSites", refFolder + SnpFileName )
	
	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!


	//	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T PrintReads -R refFolder/RefFileName -I 
	//		tmpFolder/regionX-2.bam -o tmpFolder/regionX-3.bam -BQSR tmpFolder/regionX.table -L tmpFolder/bedX.bed 

	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "GenomeAnalysisTK.jar", 
			  "-T", "PrintReads", 
			  "-R", refFolder + RefFileName, 
			  "-I", regionX2, 
			  "-o", regionX3, 
			  "-BQSR", regionTable, 
			  "-L", bedX )

	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!
	
	// delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table

	FileUtils.deleteQuietly(new File(regionX2))
	FileUtils.deleteQuietly(new File(tmpFolder + "region" + chrRegion +"-2.bai"))
	FileUtils.deleteQuietly(new File(regionTable))

	// Haplotype -> Uses the region bed file
	val regionVCF = tmpFolder + "region" + chrRegion + ".vcf"
	// java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T HaplotypeCaller -nct numOfThreads -R refFolder/RefFileName -I 
	//		tmpFolder/regionX-3.bam -o tmpFolder/regionX.vcf  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L tmpFolder/bedX.bed 
	//		--no_cmd_Seqline_in_header --disable_auto_index_creation_and_locking_when_reading_rods

	cmd_Seq = Seq("java", MemString, 
			  "-jar", toolsFolder + "GenomeAnalysisTK.jar", 
			  "-T", "HaplotypeCaller", "-nct", numOfThreads, 
			  "-R", refFolder + RefFileName, 
			  "-I", regionX3, 
			  "-o", regionVCF, 
			  "-stand_call_conf", "30.0",
			  "-stand_emit_conf", "30.0", 
			  "-L", bedX,
			  "--no_cmdline_in_header", 
			  "--disable_auto_index_creation_and_locking_when_reading_rods" )

 	writer.write(getTimeStamp() + " " + cmd_Seq.mkString +"\n")
	cmd_Seq!

	writer.close()
	// delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed
	
	FileUtils.deleteQuietly(new File(regionX3))
	FileUtils.deleteQuietly(new File(tmpFolder + "region" + chrRegion +"-3.bai"))
	FileUtils.deleteQuietly(new File(bedX))

	//From the ".vcf" file we do not consider the comments (starting with "#")
	val lines = scala.io.Source.fromFile(regionVCF)
							   .getLines
							   .filter(line => !(line.startsWith("#")))


	//I create an ArrayBuffer to store the results
	var result = new ArrayBuffer[(Integer, (Integer, String))]
	
	//We create the Array which will be returned. This Array has elements of type
	// <Chromsome number, <Chromosome Position, line>>. Here we give the numbers 23 and 24 for the 
	//Sex chromosomes X, Y respectively.
	for (line <- lines){
		var chrString = line.split("\t")(0).substring(3)
		var chrNumber = 0
		
		if (chrString=="X"){
			chrNumber = 23
		}

		else if( chrString=="Y"){
			chrNumber = 24
		}
		
		else{
			chrNumber = chrString.toInt
		}

		var pos = line.split("\t")(1).toInt

		result += ((chrNumber, (pos, line )))

	}

	// return the content of the vcf file produced by the haplotype caller.
	//	Return those in the form of <Chromsome number, <Chromosome Position, line>>
	return result.toArray // Replace this with what is described in the above 2 lines
}


def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize()
	val numOfInstances = config.getNumInstances
		 
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	// For local mode, include the following two lines
	conf.setMaster("local[" + config.getNumInstances() + "]")
	conf.set("spark.cores.max", config.getNumInstances())
	
	val sc = new SparkContext(conf)
	sc.broadcast(config)

	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	// Rest of the code goes here

	//Configuration of input and output folders
	val inputFolder = config.getInputFolder
	val outputFolder = config.getOutputFolder

 	var directory = new File(inputFolder)

 	//We create a Seq with the file names which exist in the input folder
    val filesList = directory.listFiles.filter(_.isFile).toSeq

    //RDD with Files
    val rddFilesList = sc.parallelize(filesList)
   
	
	//RDD with records <chromosome number, SAM record>
	//We create this RDD by passing every file to bwaRun. With the use of the flatMap
	//we end up with (chromosome number, SAM record) for all the records
    val chrRecord = rddFilesList.flatMap(x => bwaRun(x.toString, config)).cache
    
    //Load balancing

    //RDD with <chromosome number, Number of Records>
    //First we count how many records there are for each chromosome number
    val chrNumberOfRecords = chrRecord.map{case(chr, record) => ((chr),1)}
    									 .reduceByKey((a,b) => (a+b))


    //Here I do the loadBalancing. First, I create a Vector of size numOfInstances. The index of each element represents
    //a partition and the value of each element represents the number of records that
    // have be assigned to the specific region.
    //The logic that I used is pretty straight forward. For each chromosome number, I assign it
    //to the partition which has the lowest number of records by this time. 
    //To do that I find every time the index of the element with the lowest value,
    //I map the current chromosome number to this index, and I update the value of
    //this element by adding the number of the records of this chromosome number.
    //Finaly I transform the <chromosome number,number of records> => <chromosome number, region>
   	var partitions = Array.fill(numOfInstances.toInt)(0)
   	val chrPartition = chrNumberOfRecords.map{ case(chr, value) =>
   		
   		val index = partitions.indexOf(partitions.min)
   		partitions(index) += value
        
   		(chr, index)
   	}

   	//Now, I join the <chromosome number, SAM Record> with the 
   	//<chromosome number, region>
  	val samRecordsBalanced = chrRecord.join(chrPartition)
  									  //I create an RDD with <region, record>
   									  .map{case(chr, (record,index)) => (index, record)}
   									  //I partition the RDD according to the indexes
   									  .partitionBy(new HashPartitioner(numOfInstances.toInt))

   //Here I call the variantCall for each partition and I store the result
   //in  the RDD vcfOutput
    val vcfOutput = samRecordsBalanced.mapPartitions{iterator =>

    	//First I transform the records <chromosome number, Record> to
    	//<<chromosome number, startion position>,record> then I sort them , first,by their chromosome numbers, 
    	//and then by their starting position (getReferenceIndex and getAlignmentStart)
    	val samRecordsSorted = iterator.toArray
    		.map{case(chrNum, record) => ((record.getReferenceIndex, record.getAlignmentStart),record )}
    		.sortBy(_._1)
    		//finally I have an RDD with SAM Records (sorted as explained)
    		.map{case((chrNum, pos), samRecord) => samRecord}
    	//I call variantCall with parameters (region numver, SAM record => sorted, config)
    	variantCall(TaskContext.getPartitionId, samRecordsSorted, config).toIterator
    	}.cache

  
   //Here, I sort the output of the variant calling (again by chromosome number. and starting position)
   //and i transform the vcfOutput RDD to the take only the SAM record.
   val finalRes = vcfOutput.map{case( (chrNum,(pos, line) ) ) => ((chrNum, pos), line) }
    					.sortBy(_._1)
    					.map{case( (chrNum, pos), line )  => (line)  }
    
   
    val fw = new FileWriter(new File(outputFolder + "result.vcf"))
	fw.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tls\n")

	//I write the each line-record to the final file.
	//The differences I found with the compare.py were 16.
	var newLine = ""
		val records = finalRes.collect
    	for(line <- records){
			fw.write(line + "\n")
		}
	
	fw.close
	
	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
