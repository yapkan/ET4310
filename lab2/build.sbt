ThisBuild / scalaVersion := "2.11.12"

lazy val lab1 = (project in file("."))
	.settings(
		name := "Lab2 project",

    	libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
			libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
	)
