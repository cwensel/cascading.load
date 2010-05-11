/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load;

import java.io.File;

import cascading.flow.Flow;
import load.countsort.CountSort;
import load.generate.GenerateData;
import load.join.MultiJoin;

/**
 *
 */
public class AllLoadsTest extends LoadTestCase
  {
  String output = "build/test/output/generate/";

  public AllLoadsTest()
    {
    super( "generate data tests" );
    }

  public void testAllLoads() throws Exception
    {
    Options options = new Options();

    options.setDataGenerate( true );
    options.setDataNumFiles( 3 );
    options.setDataFileSizeMB( 1 );
    options.setWorkingRoot( output + "working" );
    options.setInputRoot( output + "input" );
    options.setOutputRoot( output + "output" );

    GenerateData generate = new GenerateData( options, getProperties() );

    Flow generateFlow = generate.createFlow();

    generateFlow.complete();

    assertEquals( 6, new File( generate.getInputPaths()[ 0 ] ).list().length );
    assertEquals( 6, new File( generate.getOutputPaths()[ 0 ] ).list().length );

    CountSort countSort = new CountSort( options, getProperties() );

    Flow countSortFlow = countSort.createFlow();

    countSortFlow.complete();

    assertEquals( 2, new File( countSort.getOutputPaths()[ 0 ] ).list().length );

    MultiJoin multiJoin = new MultiJoin( options, getProperties() );

    Flow multiJoinFlow = multiJoin.createFlow();

    multiJoinFlow.complete();

    assertEquals( 2, new File( multiJoin.getOutputPaths()[ 0 ] ).list().length );
    assertEquals( 2, new File( multiJoin.getOutputPaths()[ 1 ] ).list().length );
    assertEquals( 2, new File( multiJoin.getOutputPaths()[ 2 ] ).list().length );
    assertEquals( 2, new File( multiJoin.getOutputPaths()[ 3 ] ).list().length );
    }

  }
