Welcome

 This is the Cascading.Load application.

 It provides a simple command line interface for building high load cluster jobs.

 This application is built with Cascading.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/


Building

 This release requires at least Cascading 1.2.x and will pull all dependencies from
 the relevant maven repos, including conjars.org.

 To build a jar,

 > ant retrieve jar

 To test,

 > ant test

Using

  To run from the command line, Hadoop should be in the path:

  > hadoop jar load.jar <args>

  If no args are given, a comprehensive list of commands will be printed.

License

 See LICENSE.txt
