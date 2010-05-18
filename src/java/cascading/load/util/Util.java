/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class Util
  {
//  public static TupleEntryIterator getReader( URI inputURI ) throws IOException
//    {
//    Hfs hfs = new Hfs( new TextLine( new Fields( "line" ) ), inputURI.toString() );
//
//    JobConf jobConf = new JobConf();
//
//    jobConf.set( "fs.default.name", "file:///" ); // force default fs local
//
//    return hfs.openForRead( jobConf );
//    }
//
//  public static void populateCollection( URI inputURI, Collection<String> collection ) throws IOException
//    {
//    TupleEntryIterator reader = getReader( inputURI );
//
//    while( reader.hasNext() )
//      collection.add( reader.next().getString( "line" ).trim() );
//
//    reader.close();
//    }

//  public static void getInputURIsFromList( Options options ) throws IOException
//    {
//    TupleEntryIterator reader = getReader( options.getInputURLList() );
//
//    while( reader.hasNext() )
//      options.addInputURI( reader.next().getString( "line" ).trim() );
//
//    reader.close();
//    }

  public static String[] join( String[]... values )
    {
    List<String> list = new ArrayList<String>();

    for( String[] strings : values )
      Collections.addAll( list, strings );

    return list.toArray( new String[list.size()] );
    }

  public static String extractOrNull( Pattern pattern, String value )
    {
    Matcher matcher = pattern.matcher( value );

    if( matcher.matches() )
      return matcher.replaceFirst( "$1" );

    return null;
    }

  public static void load( Class type, String resource, Properties map ) throws IOException
    {
    load( type, resource, map, true );
    }

  public static void load( Class type, String resource, Properties map, boolean trim ) throws IOException
    {
    InputStream inputStream = type.getResourceAsStream( resource );

    if( inputStream == null )
      throw new IllegalArgumentException( "unable to load resource: " + resource + ", from: " + type.getPackage().getName() );

    map.load( inputStream );
    inputStream.close();

    if( !trim )
      return;

    Set<String> names = map.stringPropertyNames();

    for( String name : names )
      {
      String value = map.getProperty( name );

      if( value != null )
        value = value.trim();

      map.remove( name );
      map.setProperty( name.trim(), value );
      }
    }

  public static void populateCollection( Class type, String resource, List<String> list ) throws IOException
    {
    InputStream inputStream = type.getResourceAsStream( resource );

    if( inputStream == null )
      throw new IllegalArgumentException( "unable to load resource: " + resource + ", from: " + type.getPackage().getName() );

    BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );

    try
      {
      String line = reader.readLine();

      while( line != null )
        {
        list.add( line );

        line = reader.readLine();
        }
      }
    finally
      {
      try
        {
        reader.close();
        }
      catch( IOException exception )
        {
        // do nothing
        }
      }
    }

//  public static void setReducers( Properties properties ) throws IOException
//    {
//    JobConf jobConf = new JobConf();
//    JobClient jobClient = new JobClient( jobConf );
//    ClusterStatus status = jobClient.getClusterStatus();
//    int trackers = status.getTaskTrackers();
//
//    properties.setProperty( "mapred.reduce.tasks", Integer.toString( trackers * jobConf.getInt( "mapred.tasktracker.reduce.tasks.maximum", 2 ) ) );
//    }

  public static int getNumTaskTrackers( JobConf jobConf )
    {
    try
      {
      JobClient jobClient = new JobClient( jobConf );
      ClusterStatus status = jobClient.getClusterStatus();

      return status.getTaskTrackers();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "failed accessing hadoop cluster", exception );
      }
    }

  public static int getMaxConcurrentMappers()
    {
    JobConf jobConf = new JobConf();

    return getNumTaskTrackers( jobConf ) * jobConf.getInt( "mapred.tasktracker.map.tasks.maximum", 2 );
    }

  public static int getMaxConcurrentReducers()
    {
    JobConf jobConf = new JobConf();

    return getNumTaskTrackers( jobConf ) * jobConf.getInt( "mapred.tasktracker.reduce.tasks.maximum", 2 );
    }

  public static boolean hasNativeZlib()
    {
    return ZlibFactory.isNativeZlibLoaded( new JobConf() );
    }
  }
