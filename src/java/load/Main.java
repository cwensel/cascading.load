/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load;

import java.io.ByteArrayOutputStream;
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
import cascading.operation.DebugLevel;
import cascading.pipe.cogroup.CoGroupClosure;
import cascading.scheme.TextLine;
import cascading.stats.CascadeStats;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import load.countsort.CountSort;
import load.generate.GenerateData;
import load.join.MultiJoin;
import load.util.StatsPrinter;
import load.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

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

    stats.captureDetail();

    OutputStream outputStream = options.hasStatsRoot() ? new ByteArrayOutputStream() : System.out;
    PrintWriter writer = new PrintWriter( outputStream );

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

    return true;
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

    properties.setProperty( "mapred.map.tasks.speculative.execution", options.isMapSpecExec() ? "true" : "false" );
    properties.setProperty( "mapred.reduce.tasks.speculative.execution", options.isReduceSpecExec() ? "true" : "false" );

    // need to try and detect if native codecs are loaded, if so, use gzip
    if( Util.hasNativeZlib() )
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );
    else
      properties.setProperty( "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec" );

    FlowConnector.setApplicationJarClass( properties, Main.class );

    // localized hadoop deps
    // reducers = num task trackers * num reduce slots
//    Util.setReducers( properties );

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
      System.err.println( exception.getMessage() );
      printUsageAndExit( parser );
      }

//    options.prepare();

    return options;
    }

  protected static void printUsageAndExit( CmdLineParser parser )
    {
    System.err.println( String.format( "hadoop %s [options...]", Main.class.getName() ) );

    System.err.println( "" );

//    System.err.println( "Optional:" );
//    System.err.println( String.format( " env vars: %s, %s", AWS.AWS_ACCESS_KEY_ENV, AWS.AWS_SECRET_KEY_ENV ) );

    System.err.println( "Options:" );
    parser.printUsage( System.err );

    System.exit( -1 );
    }

  }
