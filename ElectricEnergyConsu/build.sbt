name := "ElectricEnergyConsu"

version := "0.1"

scalaVersion := "2.10.4"

val sparkVersion = "2.2.3"

libraryDependencies++=Seq(
  "org.scala-lang"%"scala-library"%"2.10.4",
  "org.apache.spark" % "spark-core_2.10" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion)