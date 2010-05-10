/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.generate;

import java.io.File;

import cascading.flow.Flow;
import load.LoadTestCase;
import load.Options;

/**
 *
 */
public class GenerateDataTest extends LoadTestCase
  {
  String output = "build/test/output/generate/";

  public GenerateDataTest()
    {
    super( "generate data tests" );
    }

  public void testGenerateData() throws Exception
    {
    Options options = new Options();

    options.setDataGenerate( true );
    options.setDataNumFiles( 3 );
    options.setDataFileSizeMB( 1 );
    options.setWorkingRoot( output + "working" );
    options.setInputRoot( output + "input" );

    GenerateData generate = new GenerateData( options, getProperties() );

    Flow flow = generate.createFlow();

    flow.complete();

    String[] files = new File( generate.getDictionaryPath() ).list();
    assertEquals( 6, files.length );
    }

  }
