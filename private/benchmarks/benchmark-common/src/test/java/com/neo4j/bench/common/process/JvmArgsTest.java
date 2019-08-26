/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
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
        assertThat( jvmArgs.toArgs(), contains( "-Xmx4g" ) );
        // when
        jvmArgs = jvmArgs.set( "-Xms4g" );
        // then
        assertThat( jvmArgs.toArgs(), contains( "-Xmx4g", "-Xms4g" ) );
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
        assertThat( jvmArgs.toArgs(), contains( "-Xmx8g" ) );
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
        assertThat( jvmArgs.toArgs(), contains( "-XX:-PrintFlagFinal" ) );
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
        assertThat( jvmArgs.toArgs(), contains( "-XX:NativeMemorySummary=summary" ) );
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
        assertThat( jvmArgs.toArgs(), contains( "-Dapp=name1" ) );
    }

    @Test
    public void overwriteJvmSettingIfExists()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-Xloggc:/tmp/gc.log" );
        // when
        jvmArgs = jvmArgs.set( "-Xloggc:gc.log" );
        // then
        assertThat( jvmArgs.toArgs(), contains( "-Xloggc:gc.log" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void throwErrorOnUnknownArgumentType()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        jvmArgs = jvmArgs.set( "-NativeMemorySummary" );
    }

    @Test
    public void handleQuotedArgsFromString()
    {
        List<String> jvmArgs = JvmArgs.jvmArgsFromString( "-XX:OnOutMemoryError=\"kill -9 %p\" -Xms4g -Xmx4g" );
        assertArrayEquals(
                new String[] {"-XX:OnOutMemoryError=\"kill -9 %p\"","-Xms4g","-Xmx4g"},
                jvmArgs.toArray( new String[] {} ) );
    }

    @Test
    public void handleUnquotedArgsFromString()
    {
        List<String> jvmArgs = JvmArgs.jvmArgsFromString( "-Xms4g -Xmx4g" );
        assertArrayEquals(
                new String[] {"-Xms4g","-Xmx4g"},
                jvmArgs.toArray( new String[] {} ) );
    }

    @Test
    public void handleLeadingAndTralingSpaceArgsFromString()
    {
        List<String> jvmArgs = JvmArgs.jvmArgsFromString( "  -Xms4g   -Xmx4g  " );
        assertArrayEquals(
                new String[] {"-Xms4g","-Xmx4g"},
                jvmArgs.toArray( new String[] {} ) );
    }
}
