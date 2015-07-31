1) Create a new project scala/SBT in the IntelliJ IDEA
2) Add auto import
3) Put this in the build.sbt
****************************************************************
name := "SparkOpenBabel"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.4.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
  "javax.servlet" % "javax.servlet-api" % "3.0.1"
)
****************************************************************

4) Close IntelliJ IDEA and relaunch it.
5) Go in Project Structure and in the SDK add openbabel.jar in the classpath
6) Go in src/main/scala-2.11/ and run SparkOB.scala
7) Go in Run -> Edit Configuration
Add in the VM options
-Dspark.master=local
8) Run again MySimpleApp.

Have fun computation with spark/scala/OpenBabel

