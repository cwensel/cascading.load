/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.pipeline;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.load.Options;
import cascading.load.common.Load;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/** Class Pipeline sets up a simple pipeline of operations to test the hand off between operations * */
public class Pipeline extends Load
  {
  public Pipeline( Options options, Properties properties )
    {
    super( options, properties );
    }

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = new Hfs( new TextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipeline" );

    Function function = new ExpressionFunction( new Fields( "count" ), "line.split( \"\\s\").length", String.class );
    pipe = new Each( pipe, new Fields( "line" ), function, Fields.ALL );

    for( int i = 0; i < 50; i++ )
      {
      pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "count" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "line" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "count" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity() );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity( new Fields( "line2", "count2" ) ), new Fields( "line", "count2" ) );
      pipe = new Each( pipe, new Fields( "count2" ), new Identity( new Fields( "count" ) ), new Fields( "line", "count" ) );
      }

    pipe = new GroupBy( pipe, new Fields( "count" ) );

    for( int i = 0; i < 50; i++ )
      pipe = new Every( pipe, new Fields( "count" ), new Sum( new Fields( "sum" + ( i + 1 ) ) ) );

    for( int i = 0; i < 50; i++ )
      {
      pipe = new Each( pipe, new Fields( "count" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "sum1" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "count", "sum1" ), new Identity(), Fields.SWAP );
      }

    return new FlowConnector( properties ).connect( "pipeline", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "pipeline"};
    }
  }