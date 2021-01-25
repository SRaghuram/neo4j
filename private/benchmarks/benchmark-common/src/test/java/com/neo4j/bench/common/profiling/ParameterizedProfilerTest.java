/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.Lists;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ParameterizedProfilerTest
{
    @Test
    public void parseProfilerTypesWithoutParameters()
    {
        // given
        String arg = "JFR,NO_OP";
        // when
        List<ParameterizedProfiler> allParameterizedProfilerParameters = ParameterizedProfiler.parse( arg );
        // then
        assertEquals( 2, allParameterizedProfilerParameters.size() );
        ParameterizedProfiler parameterizedProfiler = allParameterizedProfilerParameters.get( 0 );
        assertEquals( ProfilerType.JFR, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
        parameterizedProfiler = allParameterizedProfilerParameters.get( 1 );
        assertEquals( ProfilerType.NO_OP, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
    }

    @Test
    public void parseProfilerTypesWithEmptyParameters()
    {
        // given
        String arg = "JFR(),NO_OP";
        // when
        List<ParameterizedProfiler> allParameterizedProfilerParameters = ParameterizedProfiler.parse( arg );
        // then
        assertEquals( 2, allParameterizedProfilerParameters.size() );
        ParameterizedProfiler parameterizedProfiler = allParameterizedProfilerParameters.get( 0 );
        assertEquals( ProfilerType.JFR, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
        parameterizedProfiler = allParameterizedProfilerParameters.get( 1 );
        assertEquals( ProfilerType.NO_OP, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
    }

    @Test
    public void parseProfilerTypesTrimWhitespaces()
    {
        // given
        String arg = " JFR , NO_OP";
        // when
        List<ParameterizedProfiler> allParameterizedProfilerParameters = ParameterizedProfiler.parse( arg );
        // then
        assertEquals( 2, allParameterizedProfilerParameters.size() );
        ParameterizedProfiler parameterizedProfiler = allParameterizedProfilerParameters.get( 0 );
        assertEquals( ProfilerType.JFR, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
        parameterizedProfiler = allParameterizedProfilerParameters.get( 1 );
        assertEquals( ProfilerType.NO_OP, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
    }

    @Test
    public void parseProfilerTypesWithParameters()
    {
        // given
        String arg = "JFR(key0=value0,key1=value1)";
        // when
        List<ParameterizedProfiler> allParameterizedProfilerParameters = ParameterizedProfiler.parse( arg );
        // then
        assertEquals( 1, allParameterizedProfilerParameters.size() );
        ParameterizedProfiler parameterizedProfiler = allParameterizedProfilerParameters.get( 0 );
        assertEquals( ProfilerType.JFR, parameterizedProfiler.profilerType() );
        assertEquals( 2, parameterizedProfiler.parameters().size() );
        assertThat(
                parameterizedProfiler.parameters(),
                allOf(
                        hasEntry( "key0", asList( "value0" ) ),
                        hasEntry( "key1", asList( "value1" ) ) ) );
    }

    @Test
    public void parseProfilerTypesWithMixed()
    {
        // given
        String arg = "JFR(key0=value0,key1=value1),GC";
        // when
        List<ParameterizedProfiler> allParameterizedProfilerParameters = ParameterizedProfiler.parse( arg );
        // then
        ParameterizedProfiler parameterizedProfiler = allParameterizedProfilerParameters.get( 0 );
        assertEquals( ProfilerType.JFR, parameterizedProfiler.profilerType() );
        assertEquals( 2, parameterizedProfiler.parameters().size() );
        assertThat(
                parameterizedProfiler.parameters(),
                allOf(
                        hasEntry( "key0", asList( "value0" ) ),
                        hasEntry( "key1", asList( "value1" ) ) ) );
        parameterizedProfiler = allParameterizedProfilerParameters.get( 1 );
        assertEquals( ProfilerType.GC, parameterizedProfiler.profilerType() );
        assertEquals( emptyMap(), parameterizedProfiler.parameters() );
    }

    @Test
    public void missingClosingParenthesis()
    {
        // given
        String arg = "JFR(key0=value0,key1=value1,GC";
        // when
        try
        {
            ParameterizedProfiler.parse( arg );
            fail( "expecting exception" );
        }
        catch ( IllegalStateException e )
        {
            // then
            assertEquals( "unexpected \"(\" character in JFR(key0=value0,key1=value1,GC", e.getMessage() );
        }
    }

    @Test
    public void notWellformedParameters()
    {
        // given
        String arg = "JFR(key0=value0,key1),GC";
        // when
        try
        {

            ParameterizedProfiler.parse( arg );
            fail( "expecting exception" );
        }
        catch ( IllegalStateException e )
        {
            // then
            assertEquals( "key-value pair \",key1\" not well-formed key0=value0,key1", e.getMessage() );
        }
    }

    @Test
    public void toArgsSerialization()
    {
        // given
        String expected = "JFR(key=value),GC";
        // when
        List<ParameterizedProfiler> profilersParameters = ParameterizedProfiler.parse( expected );
        String actual = ParameterizedProfiler.serialize( profilersParameters );
        // then
        assertEquals( expected, actual );
    }

    @Test
    public void toJsonSerialization()
    {
        // given
        HashMap<String,List<String>> parameters = new HashMap<>();
        parameters.put( "secondaryRecording", Lists.newArrayList( "CPU", "MEM" ) );
        ParameterizedProfiler expected = new ParameterizedProfiler( ProfilerType.JFR, Collections.emptyMap() );
        String json = JsonUtil.serializeJson( expected );
        // when
        ParameterizedProfiler actual = JsonUtil.deserializeJson( json, ParameterizedProfiler.class );
        // then
        assertEquals( expected, actual );
    }
}
