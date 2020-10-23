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
        assertVersion( "3.4.0","3","3.4","3.4.0","3.4.0" );
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
        assertVersion( "4.0.0-beta","4","4.0","4.0.0","4.0.0-beta" );
        assertVersion( "4.0.0-beta9","4","4.0","4.0.0","4.0.0-beta9" );
    }

    @Test
    public void shouldSupportDropReleases()
    {
        assertVersion( "4.2.0-drop07.0", "4", "4.2", "4.2.0", "4.2.0-drop07.0" );
        assertVersion( "4.2.0-drop7.0", "4", "4.2", "4.2.0", "4.2.0-drop7.0" );
        assertVersion( "4.2.0-drop07.00", "4", "4.2", "4.2.0", "4.2.0-drop07.00" );
    }

    private void assertVersion( String versionString, String main, String minor, String patch, String full )
    {
        Version version = new Version( versionString );
        assertThat( main, equalTo( version.mainVersion() ) );
        assertThat( minor, equalTo( version.minorVersion() ) );
        assertThat( patch, equalTo( version.patchVersion() ) );
        assertThat( full, equalTo( version.fullVersion() ) );
    }
    @Test
    public void shouldNotAllowToVersionsThatWrongSeparators() throws Exception
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
