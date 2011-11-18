/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.load.common;

import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.load.Options;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

import org.apache.hadoop.mapred.JobConf;

/**
 * Provides access to Cascading platform (local or hadoop) specific implementation objects.
 *
 * Only those aspects used by cascading.load are supported.
 *
 * Not thread safe.
 */
public abstract class CascadeLoadPlatform
  {
  private static CascadeLoadPlatform hadoopCascadePlatform = null;

  private static final class HadoopCascadePlatform extends CascadeLoadPlatform
    {
    protected HadoopCascadePlatform()
      {
      // has no state
      }

    @Override
    public Tap newTap( Scheme scheme, String stringPath )
      {
      return new Hfs( scheme, stringPath );
      }

    @Override
    public Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode )
      {
      return new Hfs( scheme, stringPath, sinkMode );
      }

    @Override
    public TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException
      {
      return tap.openForWrite( new JobConf() );
      }

    @Override
    public Scheme newTextLine()
      {
      return new TextLine();
      }

    @Override
    public Scheme newTextLine( Fields sourceFields )
      {
      return new TextLine( sourceFields );
      }

    @Override
    public Scheme newTextLine( Fields sourceFields, Fields sinkFields )
      {
      return new TextLine( sourceFields, sinkFields );
      }

    @Override
    public FlowConnector newFlowConnector()
      {
      return new FlowConnector();
      }

    @Override
    public FlowConnector newFlowConnector( Map<Object,Object> properties )
      {
      return new FlowConnector( properties );
      }
    }

  public static CascadeLoadPlatform getPlatform( Options options )
    {
    if( hadoopCascadePlatform == null )
      hadoopCascadePlatform = new HadoopCascadePlatform();
    return hadoopCascadePlatform;
    }

  public abstract Tap newTap( Scheme scheme, String stringPath );

  public abstract Tap newTap( Scheme scheme, String stringPath, SinkMode sinkMode );

  public abstract TupleEntryCollector newTupleEntryCollector( Tap tap ) throws IOException;

  public abstract Scheme newTextLine();

  public abstract Scheme newTextLine( Fields sourceFields );

  public abstract Scheme newTextLine( Fields sourceFields, Fields sinkFields );

  public abstract FlowConnector newFlowConnector();

  public abstract FlowConnector newFlowConnector( Map<Object,Object> properties );
  }
