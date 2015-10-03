name := "sparkStreamingPubNub"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "com.pubnub" % "pubnub" % "3.5.2"
libraryDependencies += "org.json" % "json" % "20140107"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1"