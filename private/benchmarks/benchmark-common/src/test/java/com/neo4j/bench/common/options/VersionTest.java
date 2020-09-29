/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VersionTest
{
    @Test
    public void getMainVersion()
    {
        Version version = new Version( "3.4.0" );
        assertThat( "3", equalTo( version.mainVersion() ) );
        assertThat( "3.4", equalTo( version.minorVersion() ) );
        assertThat( "3.4.0", equalTo( version.patchVersion() ) );
        assertThat( "3.4.0", equalTo( version.fullVersion() ) );
    }

    @Test
    public void shouldNotAllowToLongVersions()
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> new Version( "3.4.0.0.0" ) );
    }

    @Test
    public void shouldNotAllowToShortVersions()
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> new Version( "3.4" ) );
    }

    @Test
    public void shouldNotAllowToVersionsThatHaveNoneNumberVales() throws Exception
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> new Version( "a.0.0" ) );
    }

    @Test
    public void shouldAllowToVersionsThatBetasAndAlphas()
    {
        Version version = new Version( "4.0.0-beta" );
        assertThat( "4", equalTo( version.mainVersion() ) );
        assertThat( "4.0", equalTo( version.minorVersion() ) );
        assertThat( "4.0.0", equalTo( version.patchVersion() ) );
        assertThat( "4.0.0-beta", equalTo( version.fullVersion() ) );
    }

    @Test
    public void shouldNotAllowToVersionsThatWrongSeparators()
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> new Version( "0-0.0" ) );
    }

    @Test
    public void serializationTest()
    {
        Version version = new Version( "3.4.15" );
        Version actualVersion = JsonUtil.deserializeJson( JsonUtil.serializeJson( version ), Version.class );
        assertEquals( version, actualVersion );
    }
}
