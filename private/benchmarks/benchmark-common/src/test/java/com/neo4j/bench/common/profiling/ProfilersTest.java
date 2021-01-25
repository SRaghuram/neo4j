/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.reflect.ClassPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ProfilersTest
{
    @Test
    void internalProfilersShouldBeStateless()
    {
        List<Class> profilers = classesThatInherit( InternalProfiler.class );
        // sanity check
        assertThat( profilers.isEmpty(), equalTo( false ) );
        for ( Class profiler : profilers )
        {
            Field[] classFields = profiler.getFields();
            assertThat( "Expected to find no fields, but found: " + Arrays.toString( classFields ), classFields.length, equalTo( 0 ) );
        }
    }

    private static List<Class> classesThatInherit( Class clazz )
    {
        try
        {
            return ClassPath.from( ClassLoader.getSystemClassLoader() )
                            .getAllClasses()
                            .stream()
                            .filter( classInfo -> classInfo.getPackageName().startsWith( "com.neo4j.bench" ) )
                            .map( ClassPath.ClassInfo::load )
                            .filter( clazz::isAssignableFrom )
                            .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error loading classes", e );
        }
    }
}
