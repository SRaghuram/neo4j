/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.vmstat;

import com.neo4j.bench.common.profiling.metrics.Point;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class VmStatReaderTest
{
    @Test
    public void shouldReadSingleLine() throws URISyntaxException
    {
        Path path = Paths.get( getClass().getResource( "/metrics/vmstat/single.log" ).toURI() );

        Map<String,List<Point>> result = VmStatReader.read( path );

        Instant instant = LocalDateTime.of( 2020, 12, 3, 13, 28, 37 ).toInstant( OffsetDateTime.now().getOffset() );
        Map<String,List<Point>> expectedResult = new HashMap<String,List<Point>>()
        {{
            put( "r", Collections.singletonList( new Point( 0, instant ) ) );
            put( "b", Collections.singletonList( new Point( 0, instant ) ) );
            put( "swpd", Collections.singletonList( new Point( 0, instant ) ) );
            put( "free", Collections.singletonList( new Point( 14220, instant ) ) );
            put( "buff", Collections.singletonList( new Point( 866, instant ) ) );
            put( "cache", Collections.singletonList( new Point( 7474, instant ) ) );
            put( "si", Collections.singletonList( new Point( 0, instant ) ) );
            put( "so", Collections.singletonList( new Point( 0, instant ) ) );
            put( "bi", Collections.singletonList( new Point( 18, instant ) ) );
            put( "bo", Collections.singletonList( new Point( 34, instant ) ) );
            put( "in", Collections.singletonList( new Point( 136, instant ) ) );
            put( "cs", Collections.singletonList( new Point( 2, instant ) ) );
            put( "us", Collections.singletonList( new Point( 5, instant ) ) );
            put( "sy", Collections.singletonList( new Point( 2, instant ) ) );
            put( "id", Collections.singletonList( new Point( 93, instant ) ) );
            put( "wa", Collections.singletonList( new Point( 0, instant ) ) );
            put( "st", Collections.singletonList( new Point( 0, instant ) ) );
        }};
        assertThat( result, equalTo( expectedResult ) );
    }

    @Test
    public void shouldReadMultipleLines() throws URISyntaxException
    {
        Path path = Paths.get( getClass().getResource( "/metrics/vmstat/multiple.log" ).toURI() );

        Map<String,List<Point>> result = VmStatReader.read( path );

        Instant instant1 = LocalDateTime.of( 2020, 12, 3, 13, 28, 37 ).toInstant( OffsetDateTime.now().getOffset() );
        Instant instant2 = LocalDateTime.of( 2020, 12, 3, 13, 28, 38 ).toInstant( OffsetDateTime.now().getOffset() );
        Map<String,List<Point>> expectedResult = new HashMap<String,List<Point>>()
        {{
            put( "r", Arrays.asList( new Point( 1, instant1 ), new Point( 3, instant2 ) ) );
            put( "b", Arrays.asList( new Point( 2, instant1 ), new Point( 4, instant2 ) ) );
        }};
        assertThat( result, equalTo( expectedResult ) );
    }
}
