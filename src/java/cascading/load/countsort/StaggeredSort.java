/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.countsort;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * Class StaggeredSort does a sort on multiple fields, staggered across the tuple.
 */
public class StaggeredSort extends Load
  {
  public StaggeredSort( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "staggered-sort" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    // Assume only need to exercise the comparison, not perform any real work
    // Generate sortable data record from one field

    pipe = new Each( pipe, new Insert( new Fields( "f0", "f1", "f2", "f3", "f4", "f5",
                                                   "f6", "f7", "f8", "f9" ),
                                       "foo_1", "foo_2", "foo_3", "foo_4", "foo_5",
                                       "foo_6", "foo_7", "foo_8", "foo_9", "foo_10" ), Fields.ALL );

    // 2nd sort on every other field
    pipe = new GroupBy( pipe, new Fields( "word" ), new Fields( "f0", "f2", "f4", "f6", "f8" ) );

    return platform.newFlowConnector( properties ).connect( "staggered-sort", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "staggeredsort"};
    }
  }
