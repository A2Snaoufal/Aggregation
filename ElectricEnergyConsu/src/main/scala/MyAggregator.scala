import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._



object MyAggregator {

  def toDate(date: String): java.util.Date = {
    //2019-01-02 23:34:32+00:00,16304593690.0
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh")
    format.parse(date)
  }

  def ToStringDate(date: java.util.Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh")
    sdf.format(date);
  }

  def toDateCanFormat(date: String): java.util.Date = {
    //2019-01-02 23:34:32+00:00,16304593690.0

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    format.parse(date.split("\\+")(0))
  }

  import java.util.Calendar

  def addHoursToJavaUtilDate(date: java.util.Date, hours: Int): java.util.Date = {
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.HOUR_OF_DAY, hours)
    calendar.getTime
  }



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElectricEnergyConsu").setMaster("local[*]") // local[*] will access all core of your machine
    val sc = new SparkContext(conf) // Create Spark Context
    //Reading the data
    val emp_data = sc.textFile("src/main/resources/eec.tar.gz").persist().filter(row => !(row.contains("time")))

    //Some Cleaning
    val splittedRdd = emp_data.map( x => {
        x.split(",")}).filter(row=>row.length==4 ).map(

      values => {



          MyRow(values(0),values(1) ,toDate(values(1).split(":")(0)), values(2).toDouble,0, values(3).toDouble)
      }
    )

    //Creating a pair RDD with key (meter + date)
    val PairRdd=splittedRdd.map(row=>((row.meter,toDateCanFormat(row.time)),row))
    //Creating another pairRDD to calculate the energy difference, the windows RDD is an RDD
    val windowPairRdd= PairRdd.map(lasthourrow => ((lasthourrow._1._1,addHoursToJavaUtilDate(lasthourrow._1._2,1) ),lasthourrow._2))

    //joigning the two RDD
    val joinedRdd= windowPairRdd.join(PairRdd)

    //CAlculating the EnergyDifference persisted
    val calculatedEnergy = joinedRdd.map(joinedRow =>
      MyRow(joinedRow._1._1, "",joinedRow._1._2,joinedRow._2._2.energy,(joinedRow._2._2.energy-joinedRow._2._1.energy) ,joinedRow._2._2.power))
    .map(row=>    ((row.meter,ToStringDate(row.timeParsed)),row)).sortBy(_._1).persist()



    // Functions for Energy

    def seqOpEnergyMax = (accumulator: Double, element: MyRow) =>
      if(accumulator > element.energyDiff) accumulator else element.energyDiff

    def combOpEnergyMax = (accumulator1: Double, accumulator2: Double) =>
      if(accumulator1 > accumulator2) accumulator1 else accumulator2

    def seqOpEnergyMin = (accumulator: Double, element: MyRow) =>
      if(accumulator < element.energyDiff) accumulator else element.energyDiff

    def combOpEnergyMin = (accumulator1: Double, accumulator2: Double) =>
      if(accumulator1 < accumulator2) accumulator1 else accumulator2


  //Functions for power

    def seqOpPowerMax = (accumulator: Double, element: MyRow) =>
      if(accumulator > element.power) accumulator else element.power

    def combOpPowerMax = (accumulator1: Double, accumulator2: Double) =>
      if(accumulator1 > accumulator2) accumulator1 else accumulator2

    def seqOpPowerMin = (accumulator: Double, element: MyRow) =>
      if(accumulator < element.power) accumulator else element.power

    def combOpPowerMin = (accumulator1: Double, accumulator2: Double) =>
      if(accumulator1 < accumulator2) accumulator1 else accumulator2


  // Function for Average
    def seqOpEnergyvg = (accumulator: (Double,Double), element: MyRow) =>
    (accumulator._1 + element.energy, accumulator._2 + 1)

    def combOpEnergyAvg = (accumulator1: (Double,Double), accumulator2: (Double,Double)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    def seqOpPowerAvg = (accumulator: (Double,Double), element: MyRow) =>
      (accumulator._1 + element.energyDiff, accumulator._2 + 1)

    def combOpPowerAvg = (accumulator1: (Double,Double), accumulator2: (Double,Double)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    val zeroVal = 0.0
    val MaxEnergy=calculatedEnergy.aggregateByKey(zeroVal)(seqOpEnergyMax, combOpEnergyMax)
    val MinEnergy=calculatedEnergy.aggregateByKey(zeroVal)(seqOpEnergyMin, combOpEnergyMin)

    val MaxPower=calculatedEnergy.aggregateByKey(zeroVal)(seqOpPowerMax, combOpPowerMax)
    val MinPower=calculatedEnergy.aggregateByKey(zeroVal)(seqOpPowerMin, combOpPowerMin)

    val zeroValAvg = (0.0,0.0)


    val AvgEnegrgy=calculatedEnergy.aggregateByKey(zeroValAvg)(seqOpEnergyvg, combOpEnergyAvg).mapValues(sumCount => 1.0 * sumCount._1 / sumCount._2)
    val AvgPower=calculatedEnergy.aggregateByKey(zeroValAvg)(seqOpPowerAvg, combOpPowerAvg).mapValues(sumCount => 1.0 * sumCount._1 / sumCount._2)


    //Join the RDD
    val finalRdd = MaxEnergy.join(MinEnergy).join(MaxPower).join(MinPower).join(AvgEnegrgy).join(AvgPower)
      //meter, date, poweraverage, avgpower, min power, max power, min energy
.map(result => (result._1._1,result._1._2, result._2._2, result._2._1._2, result._2._1._1._2,result._2._1._1._1._2,result._2._1._1._1._1._2))
      .saveAsTextFile("src/main/resources/result")

  }
}
