/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class ProfilerTypeTest
{
    private static final String ALL_PROFILER_NAMES = Arrays.stream( ProfilerType.values() )
                                                           .map( ProfilerType::name )
                                                           .collect( joining( "," ) );

    @Test
    public void shouldParseAllProfilerTypes()
    {
        List<ProfilerType> profilerTypes = ProfilerType.deserializeProfilers( ALL_PROFILER_NAMES );
        MatcherAssert.assertThat( profilerTypes.size(), CoreMatchers.equalTo( ProfilerType.values().length ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldFailToParseWhenDuplicateProfilers()
    {
        String profilerNamesWithDuplicate = ALL_PROFILER_NAMES + "," + ProfilerType.values()[0].name();
        ProfilerType.deserializeProfilers( profilerNamesWithDuplicate );
    }
}
