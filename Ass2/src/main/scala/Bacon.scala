/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted

import org.apache.spark.storage.StorageLevel




object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the input file for male actors
	val compressRDDs = false
	val storage = StorageLevel.MEMORY_ONLY
	// SparkListener must log its output in file sparkLog.txt
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))
	
	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}
	
	def main(args: Array[String]) 
	{
		val cores = args(0)				// Number of cores to use
		val inputFileM = args(1)		// Path of input file for male actors
		val inputFileF = args(2)		// Path of input file for female actors (actresses)
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		conf.set("spark.rdd.compress", compressRDDs.toString)

		val sc = new SparkContext(conf)
		

		
		// Add SparkListener's code here
		
		sc.addSparkListener(new SparkListener() 
		{
			//Use the method onStageCompleted which gives information every time
			//a stage completes or succesfully fails
			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
    		{
        		//Information of all the rdds related to this stage
        		val currStage = stageCompleted.stageInfo.rddInfos
        		//Check if each rdd is cached and print the desired information accordingly
        		currStage.foreach(rdd => { if(rdd.isCached) bw.write(getTimeStamp() + " rdd " + rdd.name +
        														   ": memsize = " + (rdd.memSize / 1000000) +
        														   "MB, rdd diskSize " + rdd.diskSize +
        														   ", numPartitions = " + rdd.numPartitions + "\n" );

                   						 else  bw.write(getTimeStamp() + rdd.name + " processed!" + "\n")
        							   }
        						)
    		}
		})

		//println("Number of cores: " + args(0))
		//println("Input files: " + inputFileM + " and " + inputFileF)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		var t0 = System.currentTimeMillis
		
		// Add your main code here

		def isAllDigits(x: String) = x forall Character.isDigit

		
		//Set of the conf1 with a "\n\n" delimiter so that each <actor><listOfMovies> is separated from the rest
		//and processed as individual.
		val conf1 = new Configuration(sc.hadoopConfiguration)
		conf1.set("textinputformat.record.delimiter", "\n\n")

		//Transform the male actors txt into an RDD with the format <actor><listOfMovies> for each actor
		val actorListOfMovies = sc.newAPIHadoopFile(inputFileM, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],conf1)
								  //split by \t.
								  .map(l => (l._2.toString.split("\t")))
								  //Take the first element (name of the actor) as the key and
								  //create a tuple with the sex of the actor "M" and a list which contains
								  // the titles (with some extra infos) of his movies (by dropping the name and
								  // splitting them by the "\n" delimiter).
								  .map(l => (l(0),("M",l.drop(1).mkString.split("\n"))))
								  .setName("rdd_actorListOfMovies")
		
		//Exactly the same logic with actorListOfMovies ^.
		val actressListOfMovies = sc.newAPIHadoopFile(inputFileF, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],conf1)
									.map(l => (l._2.toString.split("\t")))
									.map(l => (l(0),("F",l.drop(1).mkString.split("\n"))))
									.setName("rdd_actressListOfMovies")
		
		///This RDD contains ((Name, (Sex, ListOfMovies)), uniqueId) for all the actors-actresses.
		val actorsMovies = actorListOfMovies.union(actressListOfMovies)
											//This unique id will be used for the creation of
											//the actorActor Graph.
											.zipWithUniqueId
											.setName("rdd_actorsMovies")
		
		//This RDD contains (MovieTitle (Year), (Id, Name, Sex)) for every movie and actor (year>=2011).
		//The MovieTitle-Year-Description was splitted by "(" so that the first element contains 
		//only the MovieTitle and then by adding a "(" and the first for characters of the second
		//element and  a ")" the <MovieTitle (Year) Description> becomes <MovieTitle (year)>.
		val movieActor = actorsMovies.flatMap(x => x._1._2._2.map(title => (title.split("\\(")(0) + "(" + title.split("\\(")(1).take(4) + ")",
														   		( x._2, x._1._1, x._1._2._1 ))))
									 //take out the rows which contain a year which is either not valid (eg XXXX)
							 		//either before than 2011
							 		.filter(x => isAllDigits(x._1.split("\\(")(1).take(4)) &&  
							 						  x._1.split("\\(")(1).take(4).toInt>2010 &&
							 						!(x._1.contains("\"")) )
							 		.persist(storage)
							 		.setName("rdd_movieActor")
								
		
		//This RDD contains (MovieTitle (Year), ,((Id1, ActorName1, Sex1), (Id2, ActorName2, Sex2)))
		//for all the actors whi have played in the same movie. 
		val actorActor = movieActor.join(movieActor)
								   .setName("rdd_actorActor")

	/*Initial idea of how to compute actorActor, but slower for the shortestPaths function
		val movieListOfActors = movieActor.groupByKey
		val actorActor1 = movieListOfActors.flatMap(x => x._2.toSet.subsets(2))
										  .map(x => (x.head,x.last))
		val actorActor2 = actorActor1.map{case (x,y) => (y,x)}
		val actorActor = actorActor1.union(actorActor2)
		//val edges = actorActor.map ( x=> (x._1._1, x._2._1))

	*/

		//This RDD contains (Name, Id) for every actor
		val actorsIds = actorsMovies.map(x => (x._1._1,(x._2)))
									.setName("rdd_actorsIds")

		//Id of Kevin Bacon
		val baconKevin = actorsIds.lookup(KevinBacon)

		//Define the edges of the Graph which will be created.
		//The idea of the Graph is simple. Each vertex represents an actor 
		//and when two actors play in the same movie we create an edge between them
		
		val edges = actorActor.map(x => (x._2._1._1, x._2._2._1))
							  .setName("rdd_edges")
		
		val nowhere = "nowhere"

		//println("Start making the graph...")

		//Create of the Graph from edges
		val graph = Graph.fromEdgeTuples(edges, 1)


				
		//println("Graph is ready...")

		//println("Start calculating Shortest Paths...")

		//Calculate all the shortest paths with starting node Kevin Bacon
		val shortestPaths = ShortestPaths.run(graph,baconKevin)
										 //take the vertices which include Id and distance
										 .vertices
										 //remove vertices for which no path found
										 .filter({case(v, path) => path.nonEmpty})
		//println("Calculation is done...")

		//This RDD contains (Id, Distance)
		val mapping = shortestPaths.map(x => (x._1, x._2.values.head))
									//take out actors in distance greater than 6
								   .filter(x => x._2.toInt< 7)
								   .setName("rdd_mapping")

		//This RDD contains (id, (Name, Sex,))
		val idActors = movieActor.map (x => (x._2._1,(x._2._2,x._2._3)))
								 .distinct
							 	 .persist(storage)
								 .setName("rdd_idActors")
		
		//This RDD contains (Id, ( (Name, Sex), Distance))
		val results = idActors.join(mapping)
							  .persist(storage)
							  .setName("rdd_results")

		//This RDD contains (Id, ( (Name, Sex), Distance)) for Male Actors
		val rDDMales = results.filter(x => x._2._1._2=="M")
							  .persist(storage)
							  .setName("rdd_rDDMales")
		
		//This RDD contains (Id, ( (Name, Sex), Distance)) for Female Actors
		val rDDFemales = results.filter(x => x._2._1._2=="F")
								.persist(storage)
								.setName("rdd_rDDFemales")

		val numberOfMovies = movieActor.reduceByKey((a,b) => (a)).count
		
		val numberOfAllActors = idActors.count

		val numberOfActors = idActors.filter(x => x._2._2=="M").count

		val numberOfActresses = idActors.filter(x => x._2._2=="F").count

		var distanceOfActors:Array[Long] = new Array[Long](6)
		var distanceOfActresses:Array[Long] = new Array[Long](6)


		for (i <-0 to 5){
			distanceOfActors(i) = rDDMales.filter(x => x._2._2 == i+1 ).count
			distanceOfActresses(i) = rDDFemales.filter(x => x._2._2 == i+1 ).count

		}
	
		var distanceOfActorsSix:Long = 0

		var distanceOfActressesSix:Long = 0

		for (i <- 0 to 5){
			distanceOfActorsSix = distanceOfActorsSix + distanceOfActors(i)
			distanceOfActressesSix = distanceOfActressesSix + distanceOfActresses(i)
		}
		
		val totalDistanceSix = distanceOfActorsSix + distanceOfActressesSix

		println(s"Total number of actors = ${numberOfAllActors}, out of which ${numberOfActors} " +
				f"(${numberOfActors.toFloat/(numberOfAllActors) * 100}%1.2f%%) are males " +
				f"while ${numberOfActresses} (${numberOfActresses.toFloat/(numberOfAllActors) * 100}%1.2f%%) are females.")

		println(s"Total number of movies ${numberOfMovies}")

		println("\n")

		for (i <- 0 to 5){
			println(f"There are ${distanceOfActors(i)} (${distanceOfActors(i).toFloat/(numberOfActors) * 100}%1.2f%%) actors " +
			f"and ${distanceOfActresses(i)} (${distanceOfActresses(i).toFloat/(numberOfActresses) * 100}%1.2f%%) actresses at distance ${i+1} ")             
		}
	
		println("\n")
		println(f"Total number of actors from distance 1 to 6 = ${totalDistanceSix}, ratio =  (${totalDistanceSix.toFloat/(numberOfAllActors) })  " )
		println(f"Total number of male actors from distance 1 to 6 = ${distanceOfActorsSix}, ratio =  (${distanceOfActorsSix.toFloat/(numberOfActors) })  " )
		println(f"Total number of female actors from distance 1 to 6 = ${distanceOfActressesSix}, ratio =  (${distanceOfActressesSix.toFloat/(numberOfActresses) })  " )
		println("\n")

		println("List of male actors at distance 6")

		//This RDD contains the names of the male actors sorted alphabetically
		val rDDMalesOutput = rDDMales.map( x => x._2._1._1)
									 .sortBy(name => name)
									 .collect()

		for (i <- 0 until rDDMalesOutput.length) {
			println(s"$i. ${rDDMalesOutput(i)}")
		}

		println("\n")
		println("List of male actors at distance 6")

		//This RDD contains the names of the female actors sorted alphabetically
		val rDDFemalesOutput = rDDFemales.map( x => x._2._1._1)
										 .sortBy(name => name)
										 .collect()

		println("\n")
		println("List of female actors at distance 6")

		for (i <- 0 until rDDFemalesOutput.length) {
			println(s"$i. ${rDDFemalesOutput(i)}")
		}

		sc.stop() 
		bw.close()
		
		val et = (System.currentTimeMillis - t0) / 1000 
		println("{Time taken = %d mins %d secs}".format(et/60, et%60))
	} 
}
