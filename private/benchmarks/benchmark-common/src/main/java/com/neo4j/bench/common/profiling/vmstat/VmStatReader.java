/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.vmstat;

import com.neo4j.bench.common.profiling.metrics.Point;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VmStatReader
{
    private static final String SEPARATOR = "\\s+";

    public static Map<String,List<Point>> read( Path path )
    {
        List<String> lines = readAllLines( path );
        if ( lines.isEmpty() )
        {
            return Collections.emptyMap();
        }
        List<String> header = extractHeaders( lines.get( 1 ) );

        List<Map<String,Point>> results = new ArrayList<>();
        for ( String line : lines.subList( 2, lines.size() ) )
        {
            results.add( parseLine( header, line ) );
        }

        return merge( results );
    }

    private static List<String> readAllLines( Path path )
    {
        try
        {
            return Files.readAllLines( path );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static List<String> extractHeaders( String line )
    {
        String[] columns = line.trim().split( SEPARATOR );
        return Arrays.asList( columns );
    }

    private static Map<String,Point> parseLine( List<String> columns, String line )
    {
        String[] fields = line.trim().split( SEPARATOR );
        Instant timestamp = LocalDateTime.parse( getTimestamp( fields ) ).toInstant( OffsetDateTime.now().getOffset() );
        Map<String,Point> measurements = new HashMap<>();
        for ( int i = 0; i < fields.length - 2; i++ )
        {
            String column = columns.get( i );
            double value = Double.parseDouble( fields[i] );
            measurements.put( column, new Point( value, timestamp ) );
        }
        return measurements;
    }

    private static String getTimestamp( String[] fields )
    {
        return fields[fields.length - 2] + "T" + fields[fields.length - 1];
    }

    private static Map<String,List<Point>> merge( List<Map<String,Point>> results )
    {
        Map<String,List<Point>> result = new HashMap<>();
        for ( Map<String,Point> stringPointMap : results )
        {
            for ( Map.Entry<String,Point> entry : stringPointMap.entrySet() )
            {
                String label = entry.getKey();
                Point point = entry.getValue();
                List<Point> points = result.computeIfAbsent( label, ignored -> new ArrayList<>() );
                points.add( point );
            }
        }
        return result;
    }
}
