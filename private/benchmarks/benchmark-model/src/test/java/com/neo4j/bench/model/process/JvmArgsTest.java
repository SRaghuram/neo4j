/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.process;

import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    public void throwErrorOnUnknownArgumentType()
    {
        // given
        JvmArgs jvmArgs = JvmArgs.from( emptyList() );
        assertThrows( IllegalArgumentException.class, () -> jvmArgs.set( "-NativeMemorySummary" ) );
    }

    @Test
    public void handleQuotedArgsFromString()
    {
        JvmArgs jvmArgs = JvmArgs.parse( "-XX:OnOutMemoryError=\"kill -9 %p\" -Xms4g -Xmx4g" );
        assertArrayEquals(
                new String[]{"-XX:OnOutMemoryError=kill -9 %p", "-Xms4g", "-Xmx4g"},
                jvmArgs.toArgs().toArray( new String[]{} ) );
    }

    @Test
    public void handleUnquotedArgsFromStringAndTrimsWhiteSpaces()
    {
        JvmArgs jvmArgs = JvmArgs.parse( "-Xms4g -Xmx4g" );
        assertArrayEquals(
                new String[]{"-Xms4g", "-Xmx4g"},
                jvmArgs.toArgs().toArray( new String[]{} ) );
    }

    @Test
    public void handleLeadingAndTralingSpaceArgsFromString()
    {
        JvmArgs jvmArgs = JvmArgs.parse( "  -Xms4g   -Xmx4g  " );
        assertArrayEquals(
                new String[]{"-Xms4g", "-Xmx4g"},
                jvmArgs.toArgs().toArray( new String[]{} ) );
    }

    @Test
    public void handleLeadingAndTralingSpaceArgsFromQuotedString()
    {
        JvmArgs jvmArgs = JvmArgs.parse( "  -XX:OnOutMemoryError=\" kill -9 %p \"  " );
        assertArrayEquals(
                new String[]{"-XX:OnOutMemoryError= kill -9 %p "},
                jvmArgs.toArgs().toArray( new String[]{} ) );
    }

    @Test
    public void addAllArguments()
    {
        JvmArgs jvmArgs0 = JvmArgs.from( "-Xmx4g" );
        JvmArgs jvmargs1 = jvmArgs0.addAll( asList( "-Xms4g", "-XX:-PrintFlagsFinal" ) );
        assertEquals( asList( "-Xmx4g", "-Xms4g", "-XX:-PrintFlagsFinal" ), jvmargs1.toArgs() );
    }

    @Test
    public void handleAgentLibArgument()
    {
        JvmArgs jvmArgs = JvmArgs.parse( "-Xms4g -Xmx4g -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005" );
        jvmArgs = jvmArgs.set( "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:6006" );
        assertEquals(
                "-Xms4g -Xmx4g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:6006",
                jvmArgs.toArgsString() );
    }
}
