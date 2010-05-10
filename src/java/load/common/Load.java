/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.common;

import java.util.Properties;

import cascading.flow.Flow;
import load.Options;

/**
 *
 */
public abstract class Load
  {
  protected Options options;
  protected Properties properties;

  public Load( Options options, Properties properties )
    {
    this.options = options;
    this.properties = new Properties( properties );
    }

  public abstract Flow createFlow() throws Exception;

  public abstract String[] getInputPaths();

  public abstract String[] getOutputPaths();
  }
