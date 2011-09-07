/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;

import cascading.flow.Flow;
import cascading.load.countsort.CountSort;
import cascading.load.generate.GenerateData;
import cascading.load.join.MultiJoin;
import cascading.load.pipeline.Pipeline;

/**
 *
 */
public class AllLoadsTest extends LoadTestCase
  {
  String output = "build/test/output/load/";

  public AllLoadsTest()
    {
    super( "generate data tests" );
    }

  public void testAllLoads() throws Exception
    {
    String output = this.output + "api/";

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

    options.setCountSort( true );

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

    Pipeline pipeline = new Pipeline( options, getProperties() );

    Flow pipelineFlow = pipeline.createFlow();

    pipelineFlow.complete();

    assertEquals( 2, new File( pipeline.getOutputPaths()[ 0 ] ).list().length );
    }

  public void testMain() throws Exception
    {
    String output = this.output + "main/";

    String[] args = new String[]{
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "3",
      "-gs", "1",

      "-c",

      "-m",

      "-p"
    };

    assertTrue( new Main( args ).execute() );

    assertEquals( 6, new File( output + "output" ).list().length );
    }

  public void testSingleLineStatus() throws Exception
    {
    String output = this.output + "mainsls/";

    String[] args = new String[]{
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "1",
      "-gs", "1",

      "-c",

      "-m",

      "-p",

      "-SLS"
    };

    assertTrue( new Main( args ).execute() );

    FileReader fr = new FileReader( output + "status/part-00000" );
    LineNumberReader ln = new LineNumberReader( fr );
    int lineNo = ln.getLineNumber();
    while( ln.readLine() != null )
      lineNo = ln.getLineNumber();
    ln.close();
    assertEquals( 15, lineNo );
    }

  public void testCleanWorkFiles() throws Exception
    {
    String output = this.output + "maincwf/";

    String[] args = new String[]{
      "-S", output + "status",
      "-I", output + "input",
      "-W", output + "working",
      "-O", output + "output",

      "-g",
      "-gf", "1",
      "-gs", "1",

      "-c",

      "-m",

      "-p",

      "-CWF"
    };

    assertTrue( new Main( args ).execute() );

    assertEquals( 1, new File( output ).list().length );
    }

  }
