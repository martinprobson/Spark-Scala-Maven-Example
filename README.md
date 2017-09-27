
Example Maven Project for Scala
===============================

# Introduction

This archive contains an example Maven project for Scala.

# Details

The `pom.xml` contains example dependencies for : -

* SLF4J
* LOG4J (acts as logging implementation for SLF4J)
* [grizzled-slf4](https://alvinalexander.com/scala/scala-logging-grizzled-slf4j) a Scala specific wrapper for SLF4J.
* Junit for testing

Note that Scala itself is just listed as another dependency which means a global installation of Scala is not required.
The `pom.xml` builds an uber-jar containing all the dependencies by default (including Scala jars) and also includes an `exec:java` goal to run the code.

# Reference

[Maven for scala link](https://docs.scala-lang.org/tutorials/scala-with-maven.html)

