/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.join;

import java.util.Map;
import java.util.Properties;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.aggregator.First;
import cascading.operation.filter.Sample;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Class MultiJoin uses the test corpus and performs both a split of of all the words into tuples and uniques all the
 * words, and then finally joins the two streams as inner, outer, left, and right joins.
 */
public class MultiJoin extends Load
  {
  public MultiJoin( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap innerSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );
    Tap outerSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 1 ], SinkMode.REPLACE );
    Tap leftSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 2 ], SinkMode.REPLACE );
    Tap rightSink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 3 ], SinkMode.REPLACE );

    Pipe uniques = new Pipe( "unique" );

    uniques = new Each( uniques, new Fields( "line" ), new RegexSplitGenerator( new Fields( "word" ), "\\s" ) );

    uniques = new GroupBy( uniques, new Fields( "word" ) );

    uniques = new Every( uniques, new Fields( "word" ), new First( Fields.ARGS ), Fields.REPLACE );

    uniques = new Each( uniques, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe fielded = new Pipe( "fielded" );

    fielded = new Each( fielded, new Fields( "line" ), new RegexSplitter( Fields.size( options.getDataMaxWords() ), "\\s" ) );

    fielded = new Each( fielded, new Sample( 0, 0.95 ) ); // need to drop some values

    Pipe inner = new CoGroup( "inner", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new InnerJoin() );
    Pipe outer = new CoGroup( "outer", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new OuterJoin() );
    Pipe left = new CoGroup( "left", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new LeftJoin() );
    Pipe right = new CoGroup( "right", fielded, new Fields( 0 ), uniques, new Fields( "word" ), new RightJoin() );

    Pipe[] heads = Pipe.pipes( uniques, fielded );
    Map<String, Tap> sources = Cascades.tapsMap( heads, Tap.taps( source, source ) );

    Pipe[] tails = Pipe.pipes( inner, outer, left, right );
    Map<String, Tap> sinks = Cascades.tapsMap( tails, Tap.taps( innerSink, outerSink, leftSink, rightSink ) );

    return platform.newFlowConnector( properties ).connect( "multi-joins", sources, sinks, tails );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{
      options.getOutputRoot() + "inner",
      options.getOutputRoot() + "outer",
      options.getOutputRoot() + "left",
      options.getOutputRoot() + "right",
    };
    }
  }