/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class VersionTest
{
    @Test
    public void getMainVersion()
    {
        Version version = new Version( "3.4.0" );
        assertThat( "3", equalTo( version.mainVersion() ) );
        assertThat( "3.4", equalTo( version.minorVersion() ) );
        assertThat( "3.4.0", equalTo( version.patchVersion() ) );
    }

    @Test
    public void shouldNotAllowToLongVersions() throws Exception
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> {
                                           new Version( "3.4.0.0" );
                                       } );
    }

    @Test
    public void shouldNotAllowToShortVersions() throws Exception
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> {
                                           new Version( "3.4" );
                                       } );
    }

    @Test
    public void shouldNotAllowToVersionsThatHaveNoneNumberVales() throws Exception
    {
        BenchmarkUtil.assertException( IllegalArgumentException.class,
                                       () -> {
                                           new Version( "a.0.0" );
                                       } );
    }
}
