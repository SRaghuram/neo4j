/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class VersionTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

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
    public void shouldNotAllowToLongVersions() throws Exception
    {
        exception.expect( IllegalArgumentException.class );
        new Version( "3.4.0.0.0" );
    }

    @Test
    public void shouldNotAllowToShortVersions() throws Exception
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Neo4j version have always been on the form x.y.z , but this version is 3.4" );
        new Version( "3.4" );
    }

    @Test
    public void shouldNotAllowToVersionsThatHaveNoneNumberVales() throws Exception
    {
        exception.expect( IllegalArgumentException.class );
        new Version( "a.0.0" );
    }

    @Test
    public void shouldAllowToVersionsThatBetasAndAlphas() throws Exception
    {
        Version version = new Version( "4.0.0-beta" );
        assertThat( "4", equalTo( version.mainVersion() ) );
        assertThat( "4.0", equalTo( version.minorVersion() ) );
        assertThat( "4.0.0", equalTo( version.patchVersion() ) );
        assertThat( "4.0.0-beta", equalTo( version.fullVersion() ) );
    }

    @Test
    public void shouldNotAllowToVersionsThatWrongSeparators() throws Exception
    {
        exception.expect( IllegalArgumentException.class );
        new Version( "0-0.0" );
    }
}
