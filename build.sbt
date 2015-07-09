name := """tabular-transform"""

version := "1.0.0"

scalaVersion := "2.11.1"


// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.6" % "test"

// For mocking ability in tests
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2-RC1" % "test"

// For reading microsoft filetypes
libraryDependencies += "org.apache.poi" % "poi" % "3.10.+"

// For reading ooxml-based microsoft filetypes
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.10.+"

// Enable scala xml handling
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.2"

