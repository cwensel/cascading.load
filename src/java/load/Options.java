/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load;

import org.kohsuke.args4j.Option;

/**
 *
 */
public class Options
  {
  boolean debugLogging = false;
  boolean mapSpecExec = false;
  boolean reduceSpecExec = false;
  int tupleSpillThreshold = 100000;

  String inputRoot;
  String outputRoot;
  String workingRoot = "working_" + System.currentTimeMillis() + "_" + (int) Math.random() * 1000;
  String statsRoot;

  boolean dataGenerate;
  int dataNumFiles = 100;
  int dataFileSizeMB = 100;
  int dataMaxWords = 10;
  int dataMinWords = 10;
  String dataWordDelimiter = " "; // space

  boolean countSort;

  boolean multiJoin;

  public boolean isDebugLogging()
    {
    return debugLogging;
    }

  @Option(name = "-X", usage = "debug logging", required = false)
  public void setDebugLogging( boolean debugLogging )
    {
    this.debugLogging = debugLogging;
    }

  public boolean isMapSpecExec()
    {
    return mapSpecExec;
    }

  @Option(name = "-EM", usage = "enable map side speculative execution", required = false)
  public void setMapSpecExec( boolean mapSpecExec )
    {
    this.mapSpecExec = mapSpecExec;
    }

  public boolean isReduceSpecExec()
    {
    return reduceSpecExec;
    }

  @Option(name = "-ER", usage = "enable reduce side speculative execution", required = false)
  public void setReduceSpecExec( boolean reduceSpecExec )
    {
    this.reduceSpecExec = reduceSpecExec;
    }

  public int getTupleSpillThreshold()
    {
    return tupleSpillThreshold;
    }

  @Option(name = "-TS", usage = "tuple spill threshold, default 100,000", required = false)
  public void setTupleSpillThreshold( int tupleSpillThreshold )
    {
    this.tupleSpillThreshold = tupleSpillThreshold;
    }

  //////////////////////////////////

  public String getInputRoot()
    {
    return makePathDir( inputRoot );
    }

  @Option(name = "-I", usage = "load input data path (generated data arrives here)", required = true)
  public void setInputRoot( String inputRoot )
    {
    this.inputRoot = inputRoot;
    }

  public String getOutputRoot()
    {
    return makePathDir( outputRoot );
    }

  @Option(name = "-O", usage = "output path for load results", required = true)
  public void setOutputRoot( String outputRoot )
    {
    this.outputRoot = outputRoot;
    }

  public String getWorkingRoot()
    {
    return makePathDir( workingRoot );
    }

  @Option(name = "-W", usage = "input/output path for working files", required = false)
  public void setWorkingRoot( String workingRoot )
    {
    this.workingRoot = workingRoot;
    }

  public boolean hasStatsRoot()
    {
    return statsRoot != null;
    }

  public String getStatsRoot()
    {
    return makePathDir( statsRoot );
    }

  @Option(name = "-S", usage = "output path for job stats", required = false)
  public void setStatsRoot( String statsRoot )
    {
    this.statsRoot = statsRoot;
    }

  private String makePathDir( String path )
    {
    if( path == null || path.isEmpty() )
      return "./";

    if( !path.endsWith( "/" ) )
      path += "/";

    return path;
    }

//////////////////////////////////

  public boolean isDataGenerate()
    {
    return dataGenerate;
    }

  @Option(name = "-g", aliases = {"--generate"}, usage = "generate test data", required = false)
  public void setDataGenerate( boolean dataGenerate )
    {
    this.dataGenerate = dataGenerate;
    }

  public int getDataNumFiles()
    {
    return dataNumFiles;
    }

  @Option(name = "-gf", aliases = {"--generate-num-files"}, usage = "num files to create", required = false)
  public void setDataNumFiles( int dataNumFiles )
    {
    this.dataNumFiles = dataNumFiles;
    }

  public int getDataFileSizeMB()
    {
    return dataFileSizeMB;
    }

  @Option(name = "-gs", aliases = {"--generate-file-size"}, usage = "size in MB of each file", required = false)
  public void setDataFileSizeMB( int dataFileSizeMB )
    {
    this.dataFileSizeMB = dataFileSizeMB;
    }

  public int getDataMaxWords()
    {
    return dataMaxWords;
    }

  @Option(name = "-gmax", aliases = {"--generate-max-words"}, usage = "max words per line, inclusive", required = false)
  public void setDataMaxWords( int dataMaxWords )
    {
    this.dataMaxWords = dataMaxWords;
    }

  public int getDataMinWords()
    {
    return dataMinWords;
    }

  @Option(name = "-gmin", aliases = {"--generate-min-words"}, usage = "min words per line, inclusive", required = false)
  public void setDataMinWords( int dataMinWords )
    {
    this.dataMinWords = dataMinWords;
    }

  public String getDataWordDelimiter()
    {
    return dataWordDelimiter;
    }

  @Option(name = "-gd", aliases = {"--generate-word-delimiter"}, usage = "delimiter for words", required = false)
  public void setDataWordDelimiter( String dataWordDelimiter )
    {
    this.dataWordDelimiter = dataWordDelimiter;
    }

  ////////////////////////////////////////

  public boolean isCountSort()
    {
    return countSort;
    }

  @Option(name = "-c", aliases = {"--count-sort"}, usage = "run count sort load", required = false)
  public void setCountSort( boolean countSort )
    {
    this.countSort = countSort;
    }

  ////////////////////////////////////////


  public boolean isMultiJoin()
    {
    return multiJoin;
    }

  @Option(name = "-m", aliases = {"--multi-join"}, usage = "run multi join load", required = false)
  public void setMultiJoin( boolean multiJoin )
    {
    this.multiJoin = multiJoin;
    }
  }
