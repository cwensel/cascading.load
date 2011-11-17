/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load.common;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.load.Options;

/**
 *
 */
public abstract class Load
  {
  protected Options options;
  protected Properties properties;
  protected CascadeLoadPlatform platform;

  public Load( Options options, Properties properties )
    {
    this.options = options;
//    this.properties = new Properties( properties );
    this.properties = properties;
    this.platform = CascadeLoadPlatform.getPlatform( options );
    }

  public abstract Flow createFlow() throws Exception;

  public abstract String[] getInputPaths();

  public abstract String[] getOutputPaths();
  }
