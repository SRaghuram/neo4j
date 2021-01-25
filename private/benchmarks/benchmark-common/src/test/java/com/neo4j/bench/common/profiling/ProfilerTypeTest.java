/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProfilerTypeTest
{
    private static final String ALL_PROFILER_NAMES = Arrays.stream( ProfilerType.values() )
                                                           .map( ProfilerType::name )
                                                           .collect( joining( "," ) );

    @Test
    public void shouldParseAllProfilerTypes()
    {
        List<ProfilerType> profilerTypes = ProfilerType.deserializeProfilers( ALL_PROFILER_NAMES );
        assertThat( profilerTypes.size(), equalTo( ProfilerType.values().length ) );
    }

    @Test
    public void shouldFailToParseWhenDuplicateProfilers()
    {
        String profilerNamesWithDuplicate = ALL_PROFILER_NAMES + "," + ProfilerType.values()[0].name();
        assertThrows( IllegalStateException.class, () ->
        {
            ProfilerType.deserializeProfilers( profilerNamesWithDuplicate );
        } );
    }
}
