/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.load.common.CascadeLoadPlatform;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.load.countsort.CountSort;
import cascading.load.countsort.StaggeredSort;
import cascading.load.countsort.FullTupleGroup;
import cascading.load.generate.GenerateData;
import cascading.load.join.MultiJoin;
import cascading.load.join.OnlyLeftJoin;
import cascading.load.join.OnlyRightJoin;
import cascading.load.join.OnlyOuterJoin;
import cascading.load.join.OnlyInnerJoin;
import cascading.load.pipeline.Pipeline;
import cascading.load.pipeline.ChainedFunction;
import cascading.load.pipeline.ChainedAggregate;
import cascading.load.util.StatsPrinter;
import cascading.load.util.Util;
import cascading.operation.DebugLevel;
import cascading.stats.CascadeStats;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  private CascadeLoadPlatform platform;

  public static void main( String[] args ) throws Exception
    {
    new Main( args ).execute();
    }

  public Main( String[] args ) throws IOException
    {
    options = new Options();

    initOptions( args, options );

    platform = CascadeLoadPlatform.getPlatform( options );
    }

  public boolean execute() throws Exception
    {
    List<Flow> flows = new ArrayList<Flow>();

    // This is unweildy
    // todo: use reflection (?) w/ table of load classes instead

    if( options.isDataGenerate() )
      flows.add( new GenerateData( options, getDefaultProperties() ).createFlow() );

    if( options.isCountSort() )
      flows.add( new CountSort( options, getDefaultProperties() ).createFlow() );

    if( options.isMultiJoin() )
      flows.add( new MultiJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isPipeline() )
      flows.add( new Pipeline( options, getDefaultProperties() ).createFlow() );

    if( options.isStaggeredSort() )
      flows.add( new StaggeredSort( options, getDefaultProperties() ).createFlow() );

    if( options.isFullTupleGroup() )
      flows.add( new FullTupleGroup( options, getDefaultProperties() ).createFlow() );

    if( options.isChainedAggregate() )
      flows.add( new ChainedAggregate( options, getDefaultProperties() ).createFlow() );

    if( options.isChainedFunction() )
      flows.add( new ChainedFunction( options, getDefaultProperties() ).createFlow() );

    if( options.isLeftJoin() )
      flows.add( new OnlyLeftJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isRightJoin() )
      flows.add( new OnlyRightJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isInnerJoin() )
      flows.add( new OnlyInnerJoin( options, getDefaultProperties() ).createFlow() );

    if( options.isOuterJoin() )
      flows.add( new OnlyOuterJoin( options, getDefaultProperties() ).createFlow() );

    Cascade cascade = new CascadeConnector( getDefaultProperties() ).connect( flows.toArray( new Flow[ 0 ] ) );

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

    if( options.isCleanWorkFiles() )
      cleanWorkFiles();

    return true;
    }

  private void cleanWorkFiles()
    {
    // - Ask each phase to delete work files:
    //   - Save every work sink Tap & then tap.deletePath( new JobConf() )
    //   - Save list of all Load instances.
    //   - Add method that stores sink Taps to Load baseclass - noteSinkTap say.
    //   noteSinkTap will save the ref when a saving flag is set. value of
    //   this flag set via options.isCleanWorkFiles() when Load instance
    //   created: also have set/get? have noteSinkTap w/ bool param?
    //   - Each Load instance calls noteSinkTap accordingly
    //   - Add method deleteSinkTaps to Load.

    try
      {
      LOG.info( "cleaning work files" );
      FileSystem fs = FileSystem.get( new JobConf() );
      fs.delete( new Path( options.getInputRoot() ), true );
      fs.delete( new Path( options.getWorkingRoot() ), true );
      fs.delete( new Path( options.getOutputRoot() ), true );
      }
    catch( Exception exception )
      {
      LOG.error( "failed cleaning work files ", exception );
      }
    }

  private void printSummary( CascadeStats stats ) throws IOException
    {
    stats.captureDetail();

    OutputStream outputStream = options.hasStatsRoot() ? new ByteArrayOutputStream() : System.out;
    PrintWriter writer = new PrintWriter( outputStream );

    writer.println( options );

    StatsPrinter.printCascadeStats( writer, stats, options.isSinglelineStats() );

    if( options.hasStatsRoot() )
      {
      String[] lines = outputStream.toString().split( "\n" );

      Tap statsTap = platform.newTap( platform.newTextLine(), options.getStatsRoot(), SinkMode.REPLACE );

      TupleEntryCollector tapWriter = platform.newTupleEntryCollector( statsTap );

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

    properties.setProperty( HadoopFlowProcess.SPILL_THRESHOLD, Integer.toString( options.getTupleSpillThreshold() ) );

//    properties.setProperty( "mapred.output.compress", "true" );
    properties.setProperty( "mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
    properties.setProperty( "mapred.output.compression.type", "BLOCK" );
    properties.setProperty( "mapred.compress.map.output", "true" );

    // -XX:+UseParallelOldGC -XX:ParallelGCThreads=1
    properties.setProperty( "mapred.child.java.opts", "-server " + options.getChildVMOptions() );

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

    if( options.getMaxConcurrentFlows() != -1 )
      Cascade.setMaxConcurrentFlows( properties, options.getMaxConcurrentFlows() );

    if( options.getMaxConcurrentSteps() != -1 )
      Flow.setMaxConcurrentSteps( properties, options.getMaxConcurrentSteps() );

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
    catch( Exception exception )
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
