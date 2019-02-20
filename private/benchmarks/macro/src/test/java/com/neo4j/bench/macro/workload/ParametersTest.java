/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ParametersTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldParseParameters()
    {
        try ( Resources resources = new Resources() )
        {
            Path parametersFile = resources.resourceFile( "/test_workloads/test/parameters/valid_param_types.txt" );
            try ( FileParametersReader reader = new FileParametersReader( parametersFile ) )
            {
                assertTrue( reader.hasNext() );
                Map<String,Object> row = reader.next();
                assertTrue( row.containsKey( "string" ) );
                assertThat( row.get( "string" ), equalTo( "1" ) );
                assertTrue( row.containsKey( "integer" ) );
                assertThat( row.get( "integer" ), equalTo( 2 ) );
                assertTrue( row.containsKey( "long" ) );
                assertThat( row.get( "long" ), equalTo( 3L ) );
                assertTrue( row.containsKey( "float" ) );
                assertThat( row.get( "float" ), equalTo( 4.1F ) );
                assertTrue( row.containsKey( "double" ) );
                assertThat( row.get( "double" ), equalTo( 5.1D ) );
                assertTrue( row.containsKey( "stringArr" ) );
                assertThat( row.get( "stringArr" ), equalTo( newArrayList( "6", "7" ) ) );
                assertTrue( row.containsKey( "integerArr" ) );
                assertThat( row.get( "integerArr" ), equalTo( newArrayList( 8, 9 ) ) );
                assertTrue( row.containsKey( "longArr" ) );
                assertThat( row.get( "longArr" ), equalTo( newArrayList( 10L, 11L ) ) );
                assertTrue( row.containsKey( "floatArr" ) );
                assertThat( row.get( "floatArr" ), equalTo( newArrayList( 12.1F, 13.1F ) ) );
                assertTrue( row.containsKey( "doubleArr" ) );
                assertThat( row.get( "doubleArr" ), equalTo( newArrayList( 14.1D, 15.1D ) ) );
                assertTrue( reader.hasNext() );
                row = reader.next();
                assertTrue( row.containsKey( "string" ) );
                assertThat( row.get( "string" ), equalTo( "1" ) );
                assertTrue( row.containsKey( "integer" ) );
                assertThat( row.get( "integer" ), equalTo( 2 ) );
                assertTrue( row.containsKey( "long" ) );
                assertThat( row.get( "long" ), equalTo( 3L ) );
                assertTrue( row.containsKey( "float" ) );
                assertThat( row.get( "float" ), equalTo( 4.1F ) );
                assertTrue( row.containsKey( "double" ) );
                assertThat( row.get( "double" ), equalTo( 5.1D ) );
                assertTrue( row.containsKey( "stringArr" ) );
                assertThat( row.get( "stringArr" ), equalTo( newArrayList( "6" ) ) );
                assertTrue( row.containsKey( "integerArr" ) );
                assertThat( row.get( "integerArr" ), equalTo( newArrayList( 8 ) ) );
                assertTrue( row.containsKey( "longArr" ) );
                assertThat( row.get( "longArr" ), equalTo( newArrayList( 10L ) ) );
                assertTrue( row.containsKey( "floatArr" ) );
                assertThat( row.get( "floatArr" ), equalTo( newArrayList( 12.1F ) ) );
                assertTrue( row.containsKey( "doubleArr" ) );
                assertThat( row.get( "doubleArr" ), equalTo( newArrayList( 14.1D ) ) );
                assertTrue( reader.hasNext() );
                row = reader.next();
                assertTrue( row.containsKey( "string" ) );
                assertThat( row.get( "string" ), equalTo( "" ) );
                assertTrue( row.containsKey( "integer" ) );
                assertThat( row.get( "integer" ), equalTo( 2 ) );
                assertTrue( row.containsKey( "long" ) );
                assertThat( row.get( "long" ), equalTo( 3L ) );
                assertTrue( row.containsKey( "float" ) );
                assertThat( row.get( "float" ), equalTo( 4.1F ) );
                assertTrue( row.containsKey( "double" ) );
                assertThat( row.get( "double" ), equalTo( 5.1D ) );
                assertTrue( row.containsKey( "stringArr" ) );
                assertThat( row.get( "stringArr" ), equalTo( new ArrayList<>() ) );
                assertTrue( row.containsKey( "integerArr" ) );
                assertThat( row.get( "integerArr" ), equalTo( new ArrayList<>() ) );
                assertTrue( row.containsKey( "longArr" ) );
                assertThat( row.get( "longArr" ), equalTo( new ArrayList<>() ) );
                assertTrue( row.containsKey( "floatArr" ) );
                assertThat( row.get( "floatArr" ), equalTo( new ArrayList<>() ) );
                assertTrue( row.containsKey( "doubleArr" ) );
                assertThat( row.get( "doubleArr" ), equalTo( new ArrayList<>() ) );
                assertFalse( reader.hasNext() );
            }
        }
    }

    @Test
    public void shouldFailToParseParameterFilesWithInvalidParameterTypes()
    {
        try ( Resources resources = new Resources() )
        {
            Path parametersFile = resources.resourceFile( "/test_workloads/test/parameters/invalid_param_types.txt" );
            BenchmarkUtil.assertException( RuntimeException.class,
                                           () -> new FileParametersReader( parametersFile ) );
        }
    }

    @Test
    public void shouldFailToParseParameterFilesWithInvalidColumnCounts()
    {
        try ( Resources resources = new Resources() )
        {
            Path parametersFile = resources.resourceFile( "/test_workloads/test/parameters/invalid_param_column_numbers.txt" );
            try ( FileParametersReader reader = new FileParametersReader( parametersFile ) )
            {
                BenchmarkUtil.assertException( RuntimeException.class, reader::hasNext );
            }
        }
    }
}
