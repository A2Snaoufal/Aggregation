//meter,time,energy,power
//case class MyRow(meter: String, time: String, energy: String, power: String)
case class MyRow(meter: String, time:String, timeParsed: java.util.Date, energy: Double, energyDiff: Double, power: Double)

//case class MyRow(meter: String,  timeParsed: java.util.Date, energy: String, energyDiff: String, power: String)
