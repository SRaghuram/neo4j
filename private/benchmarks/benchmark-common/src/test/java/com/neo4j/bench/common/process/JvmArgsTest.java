/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertThat;

import static java.util.Collections.emptyList;

public class JvmArgsTest
{

    @Test
    public void addArgIfDoesNotExist()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        // when
        jvmArgs = jvmArgs.set( "-Xmx4g" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-Xmx4g" ) );
        // when
        jvmArgs = jvmArgs.set( "-Xms4g" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-Xmx4g", "-Xms4g" ) );
    }

    @Test
    public void overwriteMemorySettingIfExists()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-Xmx4g" );
        // when
        jvmArgs = jvmArgs.set( "-Xmx8g" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-Xmx8g" ) );
    }

    @Test
    public void overwriteBooleanArgIfExists()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-XX:+PrintFlagFinal" );
        // when
        jvmArgs = jvmArgs.set( "-XX:-PrintFlagFinal" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-XX:-PrintFlagFinal" ) );
    }

    @Test
    public void overwriteValueArgIfExists()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-XX:NativeMemorySummary=detail" );
        // when
        jvmArgs = jvmArgs.set( "-XX:NativeMemorySummary=summary" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-XX:NativeMemorySummary=summary" ) );
    }

    @Test
    public void overwritePropertyIfExists()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-Dapp=name0" );
        // when
        jvmArgs = jvmArgs.set( "-Dapp=name1" );
        // then
        assertThat( jvmArgs.toArgs(), Matchers.contains( "-Dapp=name1" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwErrorOnUnknownArgumentType()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-NativeMemorySummary" );
    }
}
