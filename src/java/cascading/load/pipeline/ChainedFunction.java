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
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * Class ChainedFunction sets up a simple pipeline of operations to test the hand off between operations
 */
public class ChainedFunction extends Load
  {
  public ChainedFunction( Options options, Properties properties )
    {
    super( options, properties );
    }

  //todo: create pipe assemblies for common test tuple creation

  @Override
  public Flow createFlow() throws Exception
    {
    Tap source = platform.newTap( platform.newTextLine( new Fields( "line" ) ), getInputPaths()[ 0 ] );
    Tap sink = platform.newTap( platform.newTextLine(), getOutputPaths()[ 0 ], SinkMode.REPLACE );

    Pipe pipe = new Pipe( "chainedfunction" );

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

    return platform.newFlowConnector( properties ).connect( "chainedfunction", source, sink, pipe );
    }

  @Override
  public String[] getInputPaths()
    {
    return new String[]{options.getInputRoot()};
    }

  @Override
  public String[] getOutputPaths()
    {
    return new String[]{options.getOutputRoot() + "chainedfunction"};
    }

  /*
              index   name    ALL     REPLACE  RESULTS  SWAP    ARGS    GROUP   VALUES  UNKNOWN
   argument     X       X       X                                         X       X         X
   declared             X       X                                 X       X       X         X
   output       X       X       X       X         X       X               X       X         X

   */

  }