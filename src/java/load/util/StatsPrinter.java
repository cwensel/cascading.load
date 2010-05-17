/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package load.util;

import java.io.PrintWriter;
import java.util.Collection;

import cascading.stats.CascadeStats;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import cascading.stats.StepStats;

/**
 *
 */
public class StatsPrinter
  {
  public static void printCascadeStats( PrintWriter writer, CascadeStats cascadeStats )
    {
    printSummaryFor( writer, "cascade", cascadeStats );

    Collection<FlowStats> flowStats = cascadeStats.getChildren();

    for( FlowStats flowStat : flowStats )
      {
      writer.println();
      printSummaryFor( writer, "flow", flowStat );

      Collection<StepStats> stepStats = flowStat.getChildren();

      for( StepStats stepStat : stepStats )
        {
        writer.println();
        printSummaryFor( writer, "step", stepStat );
        }
      }
    }

  private static void printSummaryFor( PrintWriter writer, String type, CascadingStats cascadingStats )
    {
    writer.printf( "%s: %s\n", type, cascadingStats.getName() );

    writer.printf( "finish status: %s\n", cascadingStats.getStatus() );
    writer.printf( "start:    %tT\n", cascadingStats.getStartTime() );
    writer.printf( "finished: %tT\n", cascadingStats.getFinishedTime() );
    long duration = cascadingStats.getDuration() / 1000;
    writer.printf( "duration: %d:%02d:%02d\n", duration / 3600, duration % 3600 / 60, duration % 60 );

    if( !cascadingStats.getChildren().isEmpty() )
      writer.printf( "num children: %d\n", cascadingStats.getChildren().size() );

    writer.flush();
    }
  }
