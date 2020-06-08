/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.util;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class UnitsTest
{

    @Test
    public void shouldAbbreviate()
    {
        assertThat( UnitConverter.toAbbreviation( SECONDS ), equalTo( "s" ) );
        assertThat( UnitConverter.toAbbreviation( MILLISECONDS ), equalTo( "ms" ) );
        assertThat( UnitConverter.toAbbreviation( MICROSECONDS ), equalTo( "us" ) );
        assertThat( UnitConverter.toAbbreviation( NANOSECONDS ), equalTo( "ns" ) );
    }

    @Test
    public void shouldConvertAbbreviationToTimeUnit()
    {
        assertThat( UnitConverter.toTimeUnit( "s" ), equalTo( SECONDS ) );
        assertThat( UnitConverter.toTimeUnit( "ms" ), equalTo( MILLISECONDS ) );
        assertThat( UnitConverter.toTimeUnit( "us" ), equalTo( MICROSECONDS ) );
        assertThat( UnitConverter.toTimeUnit( "ns" ), equalTo( NANOSECONDS ) );
    }
}
