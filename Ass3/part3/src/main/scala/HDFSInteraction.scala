import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.fs.FSDataInputStream;
import java.io._


object HDFSInteractions{

val conf = new Configuration();
val fs = FileSystem.get(conf);


def listOfAllFiles(pathToList:String)
 		:List[String] = 
 	{
 	val path = new Path(pathToList)
 	val status = fs.listStatus(path)
 	var list = List[String]()
 	
 	status.foreach{x => 
 		list = x.getPath.toString ::list
 		}
 	list
 }

def createDirectory(dir:String){
	 val directory = new Path(dir)

	 if(fs.exists(directory))
	 	print(f"Directory ${directory} already exists \n")
	 else
	 	fs.mkdirs(directory)
}

def copyToLocal(hdfsSrc:String, localDst:String){
	
	val src = new Path(hdfsSrc);
	val dst = new Path(localDst);
 	
	//if (!fs.exists(src))
  	//	println(f"Input file ${src} not found")

  	fs.copyToLocalFile(src, dst)

}

def copyFromLocal(localDst:String, hdfsSrc:String){

	val src = new Path(localDst);
	val dst = new Path(hdfsSrc);
  	fs.copyFromLocalFile(src, dst)	
}


 

 def newFile(file:String){
 	val theFile = new Path(file)
 	if (fs.exists(theFile)){
 		fs.delete(theFile)
 		print(f"File ${file} already exists \n")
 	}

 	fs.createNewFile(theFile)
 }
 
def writeToFile(file:String, text:String){
	val path = new Path(file)
	val fileOutputStream = fs.append(path); 
	val br = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
	br.write(text)
	br.close();
}
}