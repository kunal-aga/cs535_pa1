name := "JARCitations"

version := "0.1"

scalaVersion := "2.12.15"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.3.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + module.revision + "." + artifact.extension
}