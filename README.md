Netflix Zeno
============

![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/zeno.svg)

_Zeno is now retired.  See [hollow](http://github.com/Netflix/hollow) for Netflix's replacement technology._

Some applications have data sets with extremely low latency tolerance.  For Netflix, this applies to metadata about our movies and TV shows.  We store this data directly in RAM on the servers responsible for powering the Netflix experience.

Netflix leans heavily on the Zeno framework to manage, transport, and keep updated these gigabytes of constantly-changing data replicated across thousands of servers.  Zeno: 

* Creates compact serialized representations of a set of Java Objects.
* Automatically detects and removes duplication in a data set.
* Automatically produces a minimal set of changes required to keep data up-to-date.
* Is efficient about resource impact when deserializing data.
* Provides powerful tools to debug data sets.
* Defines a pattern for separation between data model and data operations, increasing the agility of development teams.

Zeno is highly optimized.  See [The Netflix Tech Blog](http://techblog.netflix.com/2013/12/announcing-zeno-netflixs-in-memory-data.html) for a description of how Netflix has benefited from the development and application of the Zeno framework.

Artifacts
---------
Zeno binaries are published to Maven Central.

|GroupID/Org|ArtifactID/Name|Latest Version|
|-----------|---------------|--------------|
|com.netflix.zeno|netflix-zeno|2.6|

In a Maven .pom file:

        ...
        <dependency>
        	<groupId>com.netflix.zeno</groupId>
        	<artifactId>netflix-zeno</artifactId>
        	<version>2.6</version>
        </dependency>
        ...

Documentation
-------------
Documentation is available on the [wiki](https://github.com/Netflix/zeno/wiki).

Build
-----
Zeno is built via Gradle (www.gradle.org).  To build from the command line:

       ./gradlew build


Support
-------
Zeno is actively used and maintained by the Metadata Infrastructure team at Netflix.  Issues will be addressed in a timely manner.  Support can be obtained through the [Zeno google group](https://groups.google.com/group/netflix-zeno)

