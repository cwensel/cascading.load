Welcome

 This is the Cascading.Load module.

 It provides a simple command line interface for building high load cluster jobs.

 This application is built with Cascading.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/


Building

 This release requires at least Cascading 1.1.2 built for your version
 of Hadoop. See the project site for downloads.

 To build a jar,

 > ant -Dcascading.home=... -Dhadoop.home=... jar

 To test,

 > ant -Dcascading.home=... -Dhadoop.home=... test

 where "..." is the install path of each of the dependencies.

 Optionally, a build.properties file can be created in the project root
 that defines the *.home properties above.

Using

  To run from the command line, Hadoop should be in the path:

  > hadoop jar load.jar <args>

  If no args are given, a comprehensive list of commands will be printed.

License

  Copyright (c) 2010 Concurrent, Inc.

  This work has been released into the public domain
  by the copyright holder. This applies worldwide.

  In case this is not legally possible:
  The copyright holder grants any entity the right
  to use this work for any purpose, without any
  conditions, unless such conditions are required by law.