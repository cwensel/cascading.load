/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.load;

import java.util.Properties;

import cascading.PlatformTestCase;

/**
 *
 */
public class LoadTestCase extends PlatformTestCase
  {
  public LoadTestCase()
    {
    super();
    }

  public Properties getProperties()
    {
    Properties properties = new Properties();

    properties.putAll( super.getProperties() );

    return properties;
    }
  }
