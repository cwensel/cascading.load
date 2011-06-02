/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.load.countsort.CountSort;
import cascading.load.generate.GenerateData;
import cascading.load.join.MultiJoin;
import cascading.load.pipeline.Pipeline;
import cascading.load.util.StatsPrinter;
import cascading.load.util.Util;
import cascading.operation.DebugLevel;
import cascading.pipe.cogroup.CoGroupClosure;
import cascading.scheme.TextLine;
import cascading.stats.CascadeStats;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = Logger.getLogger( Options.class );

  private Options options;

  public static void main( String[] args ) throws Exception
    {
    new Main( args ).execute();
    }

  public Main( String[] args ) throws IOException
    {
    options = new Options();

    initOptions( args, options );
    }

  public boolean execute() throws Exception
    {
    List<Flow> flows = new ArrayList<Flow>();

    if( options.isDataGenerate() )
      flows.add( new GenerateData( options, getDefaultProperties() ).createFlow() );

    if( options.isCountSort() )
      flows.add( new CountSort( options, getDefaultProperties() ).createFlow() );

    if( options.isMultiJoin() )
      flows.add( new MultiJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isPipeline() )
      flows.add( new Pipeline( options, getDefaultProperties() ).createFlow() );

    Cascade cascade = new CascadeConnector( getDefaultProperties() ).connect( flows.toArray( new Flow[0] ) );

    CascadeStats stats = cascade.getCascadeStats();

    try
      {
      cascade.complete();
      }
    catch( Exception exception )
      {
      LOG.error( "failed running cascade ", exception );

      return false;
      }

    printSummary( stats );

    return true;
    }

  private void printSummary( CascadeStats stats ) throws IOException
    {
    stats.captureDetail();

    OutputStream outputStream = options.hasStatsRoot() ? new ByteArrayOutputStream() : System.out;
    PrintWriter writer = new PrintWriter( outputStream );

    writer.println( options );

    StatsPrinter.printCascadeStats( writer, stats );

    if( options.hasStatsRoot() )
      {
      String[] lines = outputStream.toString().split( "\n" );

      Hfs statsTap = new Hfs( new TextLine(), options.getStatsRoot(), SinkMode.REPLACE );

      TupleEntryCollector tapWriter = statsTap.openForWrite( new JobConf() );

      for( String line : lines )
        tapWriter.add( new Tuple( line ) );

      tapWriter.close();
      }
    }

  protected Properties getDefaultProperties() throws IOException
    {
    Properties properties = new Properties();

    if( options.isDebugLogging() )
      properties.put( "log4j.logger", "cascading=DEBUG,load=DEBUG" );
    else
      properties.put( "log4j.logger", "cascading=INFO,load=INFO" );

    if( options.isDebugLogging() )
      FlowConnector.setDebugLevel( properties, DebugLevel.VERBOSE );
    else
      FlowConnector.setDebugLevel( properties, DebugLevel.NONE );

    properties.setProperty( CoGroupClosure.SPILL_THRESHOLD, Integer.toString( options.getTupleSpillThreshold() ) );

//    properties.setProperty( "mapred.output.compress", "true" );
    properties.setProperty( "mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
    properties.setProperty( "mapred.output.compression.type", "BLOCK" );
    properties.setProperty( "mapred.compress.map.output", "true" );

    // -XX:+UseParallelOldGC -XX:ParallelGCThreads=1
    properties.setProperty( "mapred.child.java.opts", "-server -Xmx1000m -XX:+UseParallelOldGC" );

    if( options.getNumDefaultMappers() != -1 )
      properties.setProperty( "mapred.map.tasks", Integer.toString( options.getNumDefaultMappers() ) );

    if( options.getNumDefaultReducers() != -1 )
      properties.setProperty( "mapred.reduce.tasks", Integer.toString( options.getNumDefaultReducers() ) );

    properties.setProperty( "mapred.map.tasks.speculative.execution", options.isMapSpecExec() ? "true" : "false" );
    properties.setProperty( "mapred.reduce.tasks.speculative.execution", options.isReduceSpecExec() ? "true" : "false" );

    properties.setProperty( "dfs.block.size", Long.toString( options.getBlockSizeMB() * 1024 * 1024 ) );

    // need to try and detect if native codecs are loaded, if so, use gzip
    if( Util.hasNativeZlib() )
      {
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
      LOG.info( "using native codec for gzip" );
      }
    else
      {
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec" );
      LOG.info( "native codec not found" );
      }

    for( String property : options.getHadoopProperties() )
      {
      String[] split = property.split( "=" );
      properties.setProperty( split[ 0 ], split[ 1 ] );
      }

    FlowConnector.setApplicationJarClass( properties, Main.class );

    return properties;
    }

  protected static Options initOptions( String[] args, Options options ) throws IOException
    {
    CmdLineParser parser = new CmdLineParser( options );

    try
      {
      parser.parseArgument( args );
      }
    catch( CmdLineException exception )
      {
      System.out.print( "error: " );
      System.out.println( exception.getMessage() );
      printUsageAndExit( parser );
      }

    options.prepare();

    LOG.info( options );

    return options;
    }

  private static void printCascadingVersion()
    {
    try
      {
      Properties versionProperties = new Properties();

      InputStream stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/version.properties" );
      versionProperties.load( stream );

      stream = Cascade.class.getClassLoader().getResourceAsStream( "cascading/build.number.properties" );
      if( stream != null )
        versionProperties.load( stream );

      String releaseMajor = versionProperties.getProperty( "cascading.release.major" );
      String releaseMinor = versionProperties.getProperty( "cascading.release.minor", null );
      String releaseBuild = versionProperties.getProperty( "build.number", null );
      String releaseFull = null;

      if( releaseMinor == null )
        releaseFull = releaseMajor;
      else if( releaseBuild == null )
        releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );
      else
        releaseFull = String.format( "%s.%s%s", releaseMajor, releaseMinor, releaseBuild );


      System.out.println( String.format( "Using Cascading %s", releaseFull ) );
      }
    catch( IOException exception )
      {
      System.out.println( "Unknown Cascading Version" );
      }
    }

  private static void printLicense()
    {
    try
      {
      InputStream stream = Main.class.getResourceAsStream( "/LOAD-LICENSE.txt" );
      BufferedReader reader = new BufferedReader( new InputStreamReader( stream ) );

      System.out.print( "This release is licensed under the " );

      String line = reader.readLine();

      while( line != null )
        {
        if( line.matches( "^Binary License:.*$" ) )
          {
          System.out.println( line.substring( 15 ).trim() );
          break;
          }

        line = reader.readLine();
        }

      reader.close();
      }
    catch( IOException exception )
      {
      System.out.println( "Unspecified License" );
      }
    }

  protected static void printUsageAndExit( CmdLineParser parser )
    {
    System.out.println( "cascading.load [options...]" );

    printLicense();
    printCascadingVersion();

//    System.err.println( "Optional:" );
//    System.err.println( String.format( " env vars: %s, %s", AWS.AWS_ACCESS_KEY_ENV, AWS.AWS_SECRET_KEY_ENV ) );

    System.out.println( "" );
    System.out.println( "Usage:" );
    parser.printUsage( System.out );

    System.exit( 1 );
    }

  }
