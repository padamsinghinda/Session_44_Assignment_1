import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Air_Travellers_Analysis {
     def main(args: Array[String]) { 
       
       //Create conf object
       val conf = new SparkConf()
       .setAppName("Air_Travellers_Analysis")
       .setMaster("local");
       
       //create spark context object
       val sc = new SparkContext(conf)
       
       //Create a RDD
       val Holidays = sc.textFile("/home/acadgild/Desktop/Scala_Programming/Session_44/Task 2/Dataset_Holidays.txt")
       
       // Finding out number of travels for each year
       val TotalCountByYear = Holidays
                               .map(line => line.split(","))
                               .map(n => n(5))
                               .countByValue()
       
       //sc.parallelize(TotalCountByYear.toSeq).saveAsTextFile("TotalCountByYear")
       
       // Finding out most preferred destination
       val MostPreferredDestination = Holidays
                                       .map(line => line.split(","))
                                       .map(n => (n(2)))
                                       .countByValue()
                                       .maxBy(_._2)
                                       ._1

       print("\n\ndistribution of the total number of air-travelers per year :\n" + TotalCountByYear)
       print("\n\nMost preferred destination : " + MostPreferredDestination + "\n\n")
       
       //sc.parallelize(MostPreferredDestination.toSeq).saveAsTextFile("MostPreferredDestination")
     }
}