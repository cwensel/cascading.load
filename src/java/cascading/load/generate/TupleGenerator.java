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
  float dataFileSizeMB = 100;
  int dataMaxWords = 10;
  int dataMinWords = 10;
  String dataWordDelimiter = " "; // space
  float dataMeanWords = -1;
  float dataStddevWords = -1;

  //TODO should be toplevel classes; use distribution lib?

  private abstract class IntDist
    {
    protected Random random;
    protected int max;

    IntDist( int max )
      {
      this.max = max;
      this.random = new Random( System.currentTimeMillis() );
      }

    // Next integer in 0..max from distribution.
    abstract int next();
    }

  private class UniIntDist extends IntDist
    {
    UniIntDist( int max )
      {
      super( max );
      }

    int next()
      {
      return random.nextInt( max );
      }
    }

  private class NormIntDist extends IntDist
    {
    protected float mean;
    protected float stddev;
    protected int last;

    NormIntDist( int max )
      {
      super( max );
      int halfMax = this.max / 2;
      mean = (dataMeanWords == -1 ? halfMax : dataMeanWords * halfMax + halfMax) - 1;
      stddev = dataStddevWords == -1 ? Options.DEF_DATA_STDDEV * halfMax : dataStddevWords * halfMax;
      this.last = this.max - 1;
      }

    int next()
      {
      // Using a "clamp" method, "rejection", or "modulo" results in a similar stddev,
      // at least for a large (100k) sample size.
      int res = (int) Math.round( mean + stddev * random.nextGaussian() );
      if( res < 0 )
        res = 0;
      else if( res > last )
        res = last;
      return res;
      }
    }

  // Inner class cannot have static method, otherwise would be member of IntDist.
  private IntDist getIntDist( int max )
    {
    if( dataMeanWords != -1 || dataStddevWords != -1 )
      return this.new NormIntDist( max );
    else
      return this.new UniIntDist( max );
    }

  public TupleGenerator( Options options, Fields fieldDeclaration )
    {
    super( fieldDeclaration );

    dataFileSizeMB = options.getDataFileSizeMB();
    dataMaxWords = options.getDataMaxWords();
    dataMinWords = options.getDataMinWords();
    dataWordDelimiter = options.getDataWordDelimiter();
    dataMeanWords = options.getDataMeanWords();
    dataStddevWords = options.getDataStddevWords();

    if( dataMaxWords < dataMinWords )
      throw new IllegalArgumentException( "max words must not be less than min words" );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    Tuple dictionary = functionCall.getArguments().getTuple();
    Random random = new Random( System.currentTimeMillis() );
    long currentBytes = 0;
    IntDist wordIndicies = getIntDist( dictionary.size() );

    Tuple words = new Tuple();
    Tuple output = new Tuple( "" );

    while( true )
      {
      int numWords = getRandomWords( random );

      words.clear();

      for( int i = 0; i < numWords; i++ )
        words.add( dictionary.get( wordIndicies.next() ) );

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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof TupleGenerator ) )
      return false;
    if( !super.equals( object ) )
      return false;

    TupleGenerator that = (TupleGenerator) object;

    if( Float.compare( that.dataFileSizeMB, dataFileSizeMB ) != 0 )
      return false;
    if( dataMaxWords != that.dataMaxWords )
      return false;
    if( dataMinWords != that.dataMinWords )
      return false;
    if( dataWordDelimiter != null ? !dataWordDelimiter.equals( that.dataWordDelimiter ) : that.dataWordDelimiter != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( dataFileSizeMB != +0.0f ? Float.floatToIntBits( dataFileSizeMB ) : 0 );
    result = 31 * result + dataMaxWords;
    result = 31 * result + dataMinWords;
    result = 31 * result + ( dataWordDelimiter != null ? dataWordDelimiter.hashCode() : 0 );
    return result;
    }
  }
