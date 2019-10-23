/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

class VersionTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    void getMainVersion()
    {
        Version version = new Version( "3.4.0" );
        assertThat( "3", equalTo( version.getMainVersion() ) );
        assertThat( "4", equalTo( version.getMinorVersion() ) );
        assertThat( "3.4", equalTo( version.getMainAndMinorVersion() ) );
        assertThat( "0", equalTo( version.getPatchVersion() ) );
        assertThat( "3.4.0", equalTo( version.getMainAndMinorAndPatchVersion() ) );
    }

    @Test
    void shouldNotAllowToLongVersions()
    {
        exception.expect( IllegalArgumentException.class );
        new Version( "3.4.0.0" );
    }

    @Test
    void shouldNotAllowToShortVersions()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( is( "Neo4j version have always been on the form x.xx.xx , but this version is 3.4" ) );
        new Version( "3.4" );
    }

    @Test
    void shouldNotAllowToVersionsThatHaveNoneNumberVales()
    {
        exception.expect( RuntimeException.class );
        new Version( "a.0.0" );
    }
}
