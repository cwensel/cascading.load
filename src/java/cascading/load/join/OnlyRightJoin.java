/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.join;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.filter.Sample;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.RightJoin;
import cascading.pipe.assembly.Unique;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Class OnlyRightJoin uses the test corpus and performs both a split of of all the words into tuples and uniques all the
 * words, and then finally joins the two streams as a right join.
 */
public class OnlyRightJoin extends Load
  {
  public OnlyRightJoin( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap rightSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe uniques = new Pipe( "unique" );

    uniques = new Each( uniques, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    uniques = new Unique( uniques, new Fields( "word" ) );

    uniques = new Each( uniques, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe fielded = new Pipe( "fielded" );

    fielded = new Each( fielded, new Fields( "line" ), new RegexSplitter( Fields.size( options.getDataMaxWords() ), "\\s" ) );

    fielded = new Each( fielded, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe right = new CoGroup( "right", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new RightJoin() );

    Pipe[] heads = Pipe.pipes( uniques, fielded );
    Map<String, Tap> sources = Cascades.tapsMap( heads, Tap.taps( source, source ) );

    return platform.newFlowConnector( properties ).connect( "right-join", sources, rightSink, right );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "onlyright"};
    }
  }