/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.generate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import load.Options;
import load.util.Util;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class GenerateData
  {
  Options options;
  Properties properties;
  private String dictionaryPath;

  public GenerateData( Options options, Properties properties )
    {
    this.options = options;
    this.properties = new Properties( properties );
    }

  public Flow createFlow() throws Exception
    {
    dictionaryPath = writeDictionaryTuples();

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), dictionaryPath );
    Tap sink = new Hfs( new TextLine(), options.getInputRoot(), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "load-generator" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( "\t" ) );
    pipe = new Each( pipe, new TupleGenerator( options, new Fields( "line" ) ) );

    return new FlowConnector( properties ).connect( "generate-data", source, sink, pipe );
    }

  public String getDictionaryPath()
    {
    return dictionaryPath;
    }

  private String writeDictionaryTuples() throws IOException
    {
    List<String> dictionary = new ArrayList<String>();
    Util.populateCollection( GenerateData.class, "dictionary.txt", dictionary );

    Tuple output = new Tuple();

    output.addAll( (Object[]) dictionary.toArray( new String[dictionary.size()] ) );

    String workingPath = options.getWorkingRoot() + "dictionary/";
    Tap tap = new Hfs( new TextLine(), workingPath );
    JobConf jobConf = new JobConf();

    for( int i = 0; i < options.getDataNumFiles(); i++ )
      {
      jobConf.setInt( "mapred.task.partition", i );

      TupleEntryCollector writer = tap.openForWrite( jobConf );

      writer.add( output );

      writer.close();
      }

    return workingPath;
    }
  }
