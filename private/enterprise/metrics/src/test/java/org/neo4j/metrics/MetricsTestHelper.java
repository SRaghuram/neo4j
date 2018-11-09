/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class MetricsTestHelper
{
    interface CsvField
    {
        String header();
    }

    enum GaugeField implements CsvField
    {
        TIME_STAMP( "t" ),
        METRICS_VALUE( "value" );

        private final String header;

        GaugeField( String header )
        {
            this.header = header;
        }

        @Override
        public String header()
        {
            return header;
        }
    }

    enum CounterField implements CsvField
    {
        TIME_STAMP( "t" ),
        COUNT( "count" ),
        MEAN_RATE( "mean_rate" ),
        M1_RATE( "m1_rate" ),
        M5_RATE( "m5_rate" ),
        M15_RATE( "m15_rate" ),
        RATE_UNIt( "rate_unit" );

        private final String header;

        CounterField( String header )
        {
            this.header = header;
        }

        @Override
        public String header()
        {
            return header;
        }
    }

    enum TimerField implements CsvField
    {
        T,
        COUNT,
        MAX,MEAN,MIN,STDDEV,
        P50,P75,P95,P98,P99,P999,
        MEAN_RATE,M1_RATE,M5_RATE,M15_RATE,
        RATE_UNIT,DURATION_UNIT;

        @Override
        public String header()
        {
            return name().toLowerCase();
        }
    }

    private MetricsTestHelper()
    {
    }

    public static long readLongCounterValue( File metricFile ) throws IOException, InterruptedException
    {
        return readLongCounterAndAssert( metricFile, ( one, two ) -> true );
    }

    public static long readLongGaugeValue( File metricFile ) throws IOException, InterruptedException
    {
        return readLongGaugeAndAssert( metricFile, ( one, two ) -> true );
    }

    public static long readLongGaugeAndAssert( File metricFile, BiPredicate<Long,Long> assumption )
            throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0L, GaugeField.TIME_STAMP, GaugeField.METRICS_VALUE, Long::parseLong, assumption );
    }

    public static long readLongCounterAndAssert( File metricFile, BiPredicate<Long,Long> assumption )
            throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0L, CounterField.TIME_STAMP, CounterField.COUNT, Long::parseLong, assumption );
    }

    static double readDoubleGaugeValue( File metricFile ) throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0d, GaugeField.TIME_STAMP, GaugeField.METRICS_VALUE,
                Double::parseDouble, ( one, two ) -> true );
    }

    static long readTimerLongValueAndAssert( File metricFile, BiPredicate<Long,Long> assumption, TimerField field ) throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0L, TimerField.T, field, Long::parseLong, assumption );
    }

    static double readTimerDoubleValue( File metricFile, TimerField field ) throws IOException, InterruptedException
    {
        return readTimerDoubleValueAndAssert( metricFile, ( a, b ) -> true, field );
    }

    private static double readTimerDoubleValueAndAssert( File metricFile, BiPredicate<Double,Double> assumption, TimerField field )
            throws IOException, InterruptedException
    {
        return readValueAndAssert( metricFile, 0d, TimerField.T, field, Double::parseDouble, assumption );
    }

    private static <T, FIELD extends Enum<FIELD> & CsvField> T readValueAndAssert( File metricFile, T startValue, FIELD timeStampField, FIELD metricsValue,
            Function<String,T> parser, BiPredicate<T,T> assumption ) throws IOException, InterruptedException
    {
        // let's wait until the file is in place (since the reporting is async that might take a while)
        assertEventually( "Metrics file should exist", metricFile::exists, is( true ), 40, SECONDS );

        try ( BufferedReader reader = new BufferedReader( new FileReader( metricFile ) ) )
        {
            String s;
            do
            {
                s = reader.readLine();
            }
            while ( s == null );
            String[] headers = s.split( "," );
            assertThat( headers.length, is( timeStampField.getClass().getEnumConstants().length ) );
            assertThat( headers[timeStampField.ordinal()], is( timeStampField.header() ) );
            assertThat( headers[metricsValue.ordinal()], is( metricsValue.header() ) );

            T currentValue = startValue;
            String line;

            // Always read at least one line of data
            do
            {
                line = reader.readLine();
            }
            while ( line == null );

            do
            {
                String[] fields = line.split( "," );
                T newValue = parser.apply( fields[metricsValue.ordinal()] );
                assertTrue( "assertion failed on " + newValue + " " + currentValue,
                        assumption.test( newValue, currentValue ) );
                currentValue = newValue;
            }
            while ( (line = reader.readLine()) != null );
            return currentValue;
        }
    }

    public static File metricsCsv( File dbDir, String metric ) throws InterruptedException
    {
        File csvFile = new File( dbDir, metric + ".csv" );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 40, SECONDS );
        return csvFile;
    }
}
