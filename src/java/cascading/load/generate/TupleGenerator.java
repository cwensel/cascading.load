/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.generate;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.load.Options;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
class TupleGenerator extends BaseOperation implements Function
  {
  int dataFileSizeMB = 100;
  int dataMaxWords = 10;
  int dataMinWords = 10;
  String dataWordDelimiter = " "; // space

  public TupleGenerator( Options options, Fields fieldDeclaration )
    {
    super( fieldDeclaration );

    dataFileSizeMB = options.getDataFileSizeMB();
    dataMaxWords = options.getDataMaxWords();
    dataMinWords = options.getDataMinWords();
    dataWordDelimiter = options.getDataWordDelimiter();
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    Tuple dictionary = functionCall.getArguments().getTuple();
    Random random = new Random( System.currentTimeMillis() );
    long currentBytes = 0;

    Tuple words = new Tuple();
    Tuple output = new Tuple( "" );

    while( true )
      {
      int numWords = getRandomWords( random );

      words.clear();

      for( int i = 0; i < numWords; i++ )
        words.add( dictionary.get( random.nextInt( dictionary.size() ) ) );

      String line = words.toString( dataWordDelimiter );
      currentBytes += line.getBytes().length;

      if( currentBytes > dataFileSizeMB * 1024 * 1024 ) // don't go over to prevent small blocks
        break;

      output.set( 0, line );

      functionCall.getOutputCollector().add( output );
      }
    }

  private int getRandomWords( Random random )
    {
    return dataMaxWords == dataMinWords ? dataMaxWords : random.nextInt( dataMaxWords - dataMinWords + 1 ) + dataMinWords;
    }
  }
