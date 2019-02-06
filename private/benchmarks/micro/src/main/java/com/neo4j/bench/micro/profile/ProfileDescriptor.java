/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.profile;

import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.client.profiling.ProfilerType;
import org.openjdk.jmh.profile.Profiler;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class ProfileDescriptor
{
    /**
     * Will succeed iff either (a) target dir is set & at least one profiler is enabled, or (b) target dir is not set
     * and none of the profilers are enabled.
     *
     * @return profile descriptor
     */
    public static ProfileDescriptor profileTo( Path targetDirectory, List<ProfilerType> profilerTypes )
    {
        List<Class<? extends Profiler>> profilers = profilerTypes.stream()
                                                                 .map( ProfileDescriptor::jmhProfilerFor )
                                                                 .collect( toList() );
        if ( !profilers.isEmpty() && null == targetDirectory )
        {
            throw new Kaboom( "Profilers enabled, but no profiler output directory set" );
        }
        if ( profilers.isEmpty() && null != targetDirectory )
        {
            throw new Kaboom( "Profiler output directory set, but no profilers enabled" );
        }
        return null == targetDirectory
               ? noProfile()
               : profileTo( targetDirectory, profilers.toArray( new Class[profilers.size()] ) );
    }

    private static Class<? extends Profiler> jmhProfilerFor( ProfilerType profilerType )
    {
        switch ( profilerType )
        {
        case JFR:
            return JfrProfiler.class;
        case ASYNC:
            return AsyncProfiler.class;
        default:
            throw new RuntimeException( "No JMH profiler available for profiler type: " + profilerType );
        }
    }

    public static ProfileDescriptor noProfile()
    {
        return new ProfileDescriptor( null, new ArrayList<>() );
    }

    private static ProfileDescriptor profileTo( Path targetDirectory, Class<? extends Profiler>... profilers )
    {
        if ( null == targetDirectory )
        {
            throw new Kaboom( "Profiler output directory was null" );
        }
        if ( profilers.length < 1 )
        {
            throw new Kaboom( "Must provide at least one profiler" );
        }
        return new ProfileDescriptor( targetDirectory.toFile().getAbsolutePath(), Arrays.asList( profilers ) );
    }

    private final String targetDirAbsolutePath;
    private final List<Class<? extends Profiler>> profilers;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    private ProfileDescriptor()
    {
        this( "", new ArrayList<>() );
    }

    private ProfileDescriptor( String targetDirAbsolutePath, List<Class<? extends Profiler>> profilers )
    {
        this.targetDirAbsolutePath = targetDirAbsolutePath;
        this.profilers = profilers;
    }

    public List<Class<? extends Profiler>> profilers()
    {
        return profilers;
    }

    public boolean doProfile()
    {
        return !profilers.isEmpty();
    }

    public Path targetDirectory()
    {
        return Paths.get( targetDirAbsolutePath );
    }

    @Override
    public String toString()
    {
        return (profilers.isEmpty()) ? "Profile(false)" : format( "Profile(true --> %s)", targetDirAbsolutePath );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ProfileDescriptor that = (ProfileDescriptor) o;
        return Objects.equals( targetDirAbsolutePath, that.targetDirAbsolutePath ) &&
               Objects.equals( profilers, that.profilers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( targetDirAbsolutePath, profilers );
    }
}
