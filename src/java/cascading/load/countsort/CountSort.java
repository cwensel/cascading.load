/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.countsort;

import java.util.Collections;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/** Class CountSort does a simple word count and then sorts the counts in decreasing order. */
public class CountSort extends Load
  {
  public CountSort( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count-sort" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "word" ) );

    pipe = new Every( pipe, new Fields( "word" ), new Count( new Fields( "count" ) ) );

    Fields groupFields = new Fields( "count" );

    groupFields.setComparator( "count", Collections.reverseOrder() );

    pipe = new GroupBy( pipe, groupFields, new Fields( "word" ) );

    return platform.newFlowConnector( properties ).connect( "count-sort", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "countsort"};
    }
  }
