/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint;

import com.neo4j.kernel.stresstests.transaction.checkpoint.workload.Workload;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TransactionThroughputChecker implements Workload.TransactionThroughput
{
    private final DateFormat dateFormat = newDateFormat();
    private final Map<String,Double> reports = new LinkedHashMap<>();

    @Override
    public void report( long transactions, long timeSlotMillis )
    {
        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds( timeSlotMillis );
        double throughput = (double) transactions / (double) elapsedSeconds;
        String timestamp = currentTime();
        reports.put( timestamp, throughput );
    }

    public void assertThroughput( PrintStream out )
    {
        if ( reports.isEmpty() )
        {
            out.println( "no reports" );
            return;
        }

        printThroughputReports( out );

        double average = average( reports.values() );
        out.println( "Average throughput (tx/s): " + average );

        double stdDeviation = stdDeviation( reports.values() );
        out.println( "Standard deviation (tx/s): " + stdDeviation );
        double twoStdDeviations = stdDeviation * 2.0;
        out.println( "Two standard deviations (tx/s): " + twoStdDeviations );

        int inOneStdDeviationRange = 0;
        int inTwoStdDeviationRange = 0;
        for ( double report : reports.values() )
        {
            if ( Math.abs( average - report ) <= stdDeviation )
            {
                inOneStdDeviationRange++;
                inTwoStdDeviationRange++;
            }
            else if ( Math.abs( average - report ) <= twoStdDeviations )
            {
                out.println( "Outside _one_ std deviation range: " + report );
                inTwoStdDeviationRange++;
            }
            else
            {
                out.println( "Outside _two_ std deviation range: " + report );
            }
        }

        int inOneStdDeviationRangePercentage =
                (int) ( (inOneStdDeviationRange  * 100.0) / (double) reports.size() );
        System.out.println( "Percentage inside one std deviation is: " + inOneStdDeviationRangePercentage );
        assertTrue( "Assumption is that at least 60 percent should be in one std deviation (" + stdDeviation  + ")" +
                    " range from the average (" + average + ") ", inOneStdDeviationRangePercentage >= 60 );

        int inTwoStdDeviationRangePercentage =
                (int) ( (inTwoStdDeviationRange  * 100.0) / (double) reports.size() );
        System.out.println( "Percentage inside two std deviations is: " + inTwoStdDeviationRangePercentage );
        assertTrue( "Assumption is that at least 90 percent should be in two std deviations (" + twoStdDeviations + ")" +
                    " range from the average (" + average + ") ", inTwoStdDeviationRangePercentage >= 90 );
    }

    private void printThroughputReports( PrintStream out )
    {
        out.println( "Throughput reports (tx/s):" );

        for ( Map.Entry<String,Double> entry : reports.entrySet() )
        {
            out.println( "\t" + entry.getKey() + "  " + entry.getValue() );
        }

        out.println();
    }

    private static double average( Collection<Double> values )
    {
        double sum = 0;
        for ( Double value : values )
        {
            sum += value;
        }
        return sum / values.size();
    }

    private static double stdDeviation( Collection<Double> values )
    {
        double average = average( values );
        double powerSum = 0;
        for ( double value : values )
        {
            powerSum += Math.pow( value - average, 2 );
        }
        return Math.sqrt( powerSum / (double) values.size() );
    }

    private String currentTime()
    {
        return dateFormat.format( new Date() );
    }

    private static DateFormat newDateFormat()
    {
        DateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSZ" );
        TimeZone timeZone = TimeZone.getTimeZone( "UTC" );
        format.setTimeZone( timeZone );
        return format;
    }
}
