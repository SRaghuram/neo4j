/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.reflect.ClassPath;
import com.neo4j.bench.common.profiling.ScheduledProfiler.FixedRate;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ProfilersTest
{
    @Test
    public void internalProfilersShouldBeStateless()
    {
        List<Class<?>> profilers = classesThatInherit( InternalProfiler.class );
        // sanity check
        assertThat( profilers.isEmpty(), equalTo( false ) );
        for ( Class<?> profiler : profilers )
        {
            Field[] classFields = profiler.getFields();
            assertThat( "Expected to find no fields, but found: " + Arrays.toString( classFields ), classFields.length, equalTo( 0 ) );
        }
    }

    @Test
    public void onlyOnScheduleMethodsShouldHaveFixedRateAnnotation()
    {
        List<Class<?>> profilers = classesThatInherit( ScheduledProfiler.class );
        // sanity check
        assertThat( profilers.isEmpty(), equalTo( false ) );
        for ( Class<?> profiler : profilers )
        {
            List<Method> methods = Arrays.stream( profiler.getMethods() )
                    .filter( method -> !"onSchedule".equals( method.getName() ) )
                    .filter( method -> method.getAnnotation( FixedRate.class ) != null )
                    .collect( Collectors.toList() );
            assertTrue(
                    format( "only onSchedule methods in class %s should have FixedRate annotation, but found %s methods",
                            profiler,
                            methods ),
                    methods.isEmpty() );
        }
    }

    private static List<Class<?>> classesThatInherit( Class<?> clazz )
    {
        try
        {
            return ClassPath.from( ClassLoader.getSystemClassLoader() ).getAllClasses().stream()
                    .filter( classInfo -> classInfo.getPackageName().startsWith( "com.neo4j.bench" ) )
                    .map( ClassPath.ClassInfo::load ).filter( clazz::isAssignableFrom ).collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error loading classes", e );
        }
    }
}
