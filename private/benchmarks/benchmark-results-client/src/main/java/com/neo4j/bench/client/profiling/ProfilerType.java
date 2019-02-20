/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.profiling;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public enum ProfilerType
{
    ASYNC(
            AsyncProfiler.class,
            RecordingType.ASYNC,
            Sets.newHashSet( AsyncProfiler.ASYNC_PROFILER_DIR_ENV_VAR ),
            new SecondaryRecordingCreator.AsyncFlameGraphCreator() ),
    GC(
            GcProfiler.class,
            RecordingType.GC_LOG,
            // requires no environment variables
            Sets.newHashSet(),
            new SecondaryRecordingCreator.GcLogProcessor() ),
    JFR(
            JfrProfiler.class,
            RecordingType.JFR,
            // requires no environment variables
            Sets.newHashSet(),
            new SecondaryRecordingCreator.JfrFlameGraphCreator() ),
    STRACE(
            StraceTracer.class,
            RecordingType.TRACE_STRACE,
            // requires no environment variables
            Sets.newHashSet(),
            SecondaryRecordingCreator.NONE ),
    JVM_LOGGING(
            JvmTracer.class,
            RecordingType.TRACE_JVM,
            // requires no environment variables
            Sets.newHashSet(),
            SecondaryRecordingCreator.NONE ),
    MP_STAT(
            MpStatTracer.class,
            RecordingType.TRACE_MPSTAT,
            // requires no environment variables
            Sets.newHashSet(),
            SecondaryRecordingCreator.NONE ),
    IO_STAT(
            IoStatTracer.class,
            RecordingType.TRACE_IOSTAT,
            // requires no environment variables
            Sets.newHashSet(),
            SecondaryRecordingCreator.NONE ),
    VM_STAT(
            VmStatTracer.class,
            RecordingType.TRACE_VMSTAT,
            // requires no environment variables
            Sets.newHashSet(),
            SecondaryRecordingCreator.NONE );

    private final Class<? extends Profiler> profiler;
    private final RecordingType primaryRecording;
    private final SecondaryRecordingCreator secondaryRecordingCreator;
    private final Set<RecordingType> secondaryRecordings;
    private final Set<String> requiredEnvironmentVariables;

    ProfilerType( Class<? extends Profiler> profiler,
                  RecordingType primaryRecording,
                  Set<String> requiredEnvironmentVariables,
                  SecondaryRecordingCreator secondaryRecordingCreator )
    {
        this.profiler = profiler;
        this.primaryRecording = primaryRecording;
        this.requiredEnvironmentVariables = requiredEnvironmentVariables;
        this.secondaryRecordingCreator = secondaryRecordingCreator;
        this.secondaryRecordings = secondaryRecordingCreator.recordingTypes();
    }

    public RecordingType recordingType()
    {
        if ( null == primaryRecording )
        {
            throw new RuntimeException( format( "Profiler '%s' has no primary recording type\n" +
                                                "But has secondary recording types: %s", name(), secondaryRecordings ) );
        }
        return primaryRecording;
    }

    public List<RecordingType> allRecordingTypes()
    {
        List<RecordingType> recordingTypes = new ArrayList<>( secondaryRecordings );
        if ( null != primaryRecording )
        {
            recordingTypes.add( primaryRecording );
        }
        return recordingTypes;
    }

    public Optional<SecondaryRecordingCreator> maybeSecondaryRecordingCreator()
    {
        if ( hasSecondaryRecordingCreator() )
        {
            List<String> missingSecondaryEnvironmentVariables = missingSecondaryEnvironmentVariables();
            if ( !missingSecondaryEnvironmentVariables.isEmpty() )
            {
                String sorryMessage = "-----------------------------------------------------------------------------------------------------------\n" +
                                      "-----------------------------------  NO SECONDARY RECORDINGS FOR YOU  -------------------------------------\n" +
                                      "-----------------------------------------------------------------------------------------------------------\n" +
                                      "Sorry, I ('" + name() + "' profiler) am unable to generate " + secondaryRecordings + " from my profiler recordings\n" +
                                      "You are missing some environment variables that I need: " + missingSecondaryEnvironmentVariables + "\n" +
                                      "-----------------------------------------------------------------------------------------------------------";
                System.out.println( sorryMessage );
                return Optional.empty();
            }
            else
            {
                return Optional.of( secondaryRecordingCreator );
            }
        }
        else
        {
            return Optional.empty();
        }
    }

    public boolean isSupportedRecordingType( RecordingType recordingType )
    {
        return (null != primaryRecording && primaryRecording.equals( recordingType )) || secondaryRecordings.contains( recordingType );
    }

    public void assertEnvironmentVariablesPresent( boolean errorOnMissingSecondaryEnvironmentVariables )
    {
        Map<String,String> environmentVariables = System.getenv();
        List<String> missingEnvironmentVariables = requiredEnvironmentVariables.stream()
                                                                               .filter( envVar -> !environmentVariables.containsKey( envVar ) )
                                                                               .collect( toList() );
        List<String> missingSecondaryEnvironmentVariables = missingSecondaryEnvironmentVariables();
        if ( !missingEnvironmentVariables.isEmpty() || (!missingSecondaryEnvironmentVariables.isEmpty() && errorOnMissingSecondaryEnvironmentVariables) )
        {
            throw new RuntimeException( format( "`%s` profiler requires certain environment variables to be set, but some were missing\n" +
                                                "  * Requires (Profiler)  : %s\n" +
                                                "  * Requires (Secondary) : %s\n" +
                                                "  * Missing (Profiler)   : %s\n" +
                                                "  * Missing (Secondary)  : %s",
                                                name(),
                                                requiredEnvironmentVariables,
                                                secondaryRecordingCreator.requiredEnvironmentVariables(),
                                                missingEnvironmentVariables,
                                                missingSecondaryEnvironmentVariables ) );
        }
    }

    private List<String> missingSecondaryEnvironmentVariables()
    {
        if ( hasSecondaryRecordingCreator() )
        {
            Map<String,String> environmentVariables = System.getenv();
            return secondaryRecordingCreator.requiredEnvironmentVariables().stream()
                                            .filter( envVar -> !environmentVariables.containsKey( envVar ) )
                                            .collect( toList() );
        }
        else
        {
            return Collections.emptyList();
        }
    }

    private boolean hasSecondaryRecordingCreator()
    {
        return null != secondaryRecordingCreator;
    }

    public Profiler create()
    {
        try
        {
            return profiler.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to create instance of: " + profiler.getName(), e );
        }
    }

    public boolean isExternal()
    {
        return ExternalProfiler.class.isAssignableFrom( profiler );
    }

    public boolean isInternal()
    {
        return InternalProfiler.class.isAssignableFrom( profiler );
    }

    public static List<ProfilerType> internalProfilers()
    {
        return Arrays.stream( ProfilerType.values() ).filter( ProfilerType::isInternal ).collect( toList() );
    }

    public static List<ProfilerType> externalProfilers()
    {
        return Arrays.stream( ProfilerType.values() ).filter( ProfilerType::isExternal ).collect( toList() );
    }

    public static String serializeProfilers( List<ProfilerType> profilerTypes )
    {
        return profilerTypes.stream().map( ProfilerType::name ).collect( joining( "," ) );
    }

    public static List<ProfilerType> deserializeProfilers( String profilerTypes )
    {
        return Arrays.stream( profilerTypes.split( "," ) )
                     .filter( profilerName -> !profilerName.isEmpty() )
                     .map( ProfilerType::valueOf )
                     .collect( toList() );
    }
}
