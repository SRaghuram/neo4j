/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

public final class MetricsTestHelper
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
        RATE_UNIT( "rate_unit" );

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
            return name().toLowerCase( Locale.ROOT );
        }
    }

    private MetricsTestHelper()
    {
    }

    public static long readLongCounterValue( Path metricFile ) throws IOException
    {
        return readLongCounterAndAssert( metricFile, ( one, two ) -> true );
    }

    public static long readLongGaugeValue( Path metricFile ) throws IOException
    {
        return readLongGaugeAndAssert( metricFile, ( one, two ) -> true );
    }

    public static long readLongGaugeAndAssert( Path metricFile, BiPredicate<Long,Long> assumption )
            throws IOException
    {
        return readValueAndAssert( metricFile, 0L, GaugeField.TIME_STAMP, GaugeField.METRICS_VALUE, Long::parseLong, assumption );
    }

    public static long readLongCounterAndAssert( Path metricFile, BiPredicate<Long,Long> assumption )
            throws IOException
    {
        return readValueAndAssert( metricFile, 0L, CounterField.TIME_STAMP, CounterField.COUNT, Long::parseLong, assumption );
    }

    public static long readLongCounterAndAssert( Path metricFile, long startValue, BiPredicate<Long,Long> assumption )
            throws IOException
    {
        return readValueAndAssert( metricFile, startValue, CounterField.TIME_STAMP, CounterField.COUNT, Long::parseLong, assumption );
    }

    static double readDoubleGaugeValue( Path metricFile ) throws IOException
    {
        return readValueAndAssert( metricFile, 0d, GaugeField.TIME_STAMP, GaugeField.METRICS_VALUE,
                Double::parseDouble, ( one, two ) -> true );
    }

    static long readTimerLongValueAndAssert( Path metricFile, BiPredicate<Long,Long> assumption, TimerField field ) throws IOException
    {
        return readValueAndAssert( metricFile, 0L, TimerField.T, field, Long::parseLong, assumption );
    }

    static double readTimerDoubleValue( Path metricFile, TimerField field ) throws IOException
    {
        return readTimerDoubleValueAndAssert( metricFile, ( a, b ) -> true, field );
    }

    private static double readTimerDoubleValueAndAssert( Path metricFile, BiPredicate<Double,Double> assumption, TimerField field )
            throws IOException
    {
        return readValueAndAssert( metricFile, 0d, TimerField.T, field, Double::parseDouble, assumption );
    }

    private static <T, FIELD extends Enum<FIELD> & CsvField> T readValueAndAssert( Path metricFile, T startValue, FIELD timeStampField, FIELD metricsValue,
            Function<String,T> parser, BiPredicate<T,T> assumption ) throws IOException
    {
        // let's try until the file is in place (since the reporting is async that might take a while)
        long endTime = currentTimeMillis() + MINUTES.toMillis( 2 );
        while ( currentTimeMillis() < endTime )
        {
            try ( BufferedReader reader = Files.newBufferedReader( metricFile, StandardCharsets.UTF_8 ) )
            {
                String headerLine = reader.readLine();
                if ( headerLine == null )
                {
                    // In the middle of a rotation where the file has been created, but the header has not yet been written
                    continue;
                }

                String[] headers = headerLine.split( "," );
                assertThat( headers.length, is( timeStampField.getClass().getEnumConstants().length ) );
                assertThat( headers[timeStampField.ordinal()], is( timeStampField.header() ) );
                assertThat( headers[metricsValue.ordinal()], is( metricsValue.header() ) );

                T currentValue = startValue;
                String line;

                // Always read at least one line of data
                boolean dataFound = false;
                while ( (line = reader.readLine()) != null )
                {
                    String[] fields = line.split( "," );
                    T newValue = parser.apply( fields[metricsValue.ordinal()] );

                    //this needs to be this tricky assertion and not assertTrue to make it junit version independent
                    assertThat( "assertion failed on " + newValue + " " + currentValue, true, is( assumption.test( newValue, currentValue ) ) );
                    currentValue = newValue;
                    dataFound = true;
                }
                if ( dataFound )
                {
                    return currentValue;
                }
            }
            catch ( NoSuchFileException e )
            {
                // File not there a.t.m. just keep retrying
            }
        }
        return startValue;
    }

    public static Path metricsCsv( Path dbDir, String metric )
    {
        Path csvFile = dbDir.resolve( metric + ".csv" );
        assertEventually( "Metrics file should exist", fileExistAndHasDataLines(csvFile), TRUE, 2, MINUTES );
        return csvFile;
    }

    private static Callable<Boolean> fileExistAndHasDataLines( Path file )
    {
        return () ->
        {
            try
            {
                return Files.exists( file ) && (Files.readAllLines( file ).size() > 1);
            }
            catch ( IOException e )
            {
                return Boolean.FALSE;
            }
        };
    }
}
