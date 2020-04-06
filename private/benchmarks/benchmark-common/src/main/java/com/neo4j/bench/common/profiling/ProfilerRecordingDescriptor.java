/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.profiling.RecordingType;

import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.common.results.RunPhase.WARMUP;
import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;

public class ProfilerRecordingDescriptor
{
    private static final String ADDITIONAL_PARAMS_SEPARATOR = ".";

    public enum Match
    {
        UNRECOGNIZED,
        IS_SANITIZED,
        RECOGNIZED
    }

    public static class ParseResult
    {
        private final Match match;
        private final Parameters additionalParameters;

        private ParseResult( Match match, Parameters additionalParameters )
        {
            this.match = match;
            this.additionalParameters = additionalParameters;
        }

        public boolean isMatch()
        {
            return match == Match.RECOGNIZED;
        }

        public Match match()
        {
            return match;
        }

        public Parameters additionalParameters()
        {
            return additionalParameters;
        }
    }

    public static ParseResult tryParse( String filename,
                                        RecordingType recordingType,
                                        BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark,
                                        RunPhase runPhase )
    {
        FullBenchmarkName benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark );
        String validPrefix = name( benchmarkName, runPhase, Parameters.NONE );
        String validSuffix = recordingType.extension();

        boolean isSanitized = filename.startsWith( sanitize( validPrefix ) ) && filename.endsWith( sanitize( validSuffix ) );
        if ( isSanitized )
        {
            // can not be parsed because sanitizing is lossy
            return new ParseResult( Match.IS_SANITIZED, null );
        }

        boolean isInvalidPrefix = !filename.startsWith( validPrefix );
        boolean isInvalidSuffix = !filename.endsWith( validSuffix );
        // warmup runs have additional run-phase modifier in their recording names, but have same prefix as measurement runs
        // make sure to filter out warmup recordings, when looking for measurement recordings
        String wrongPhasePrefix = runPhase.equals( WARMUP ) ? null : name( benchmarkName, WARMUP, Parameters.NONE );
        boolean isWrongPhase = wrongPhasePrefix != null && filename.startsWith( wrongPhasePrefix );

        if ( isInvalidPrefix || isInvalidSuffix || isWrongPhase )
        {
            return new ParseResult( Match.UNRECOGNIZED, null );
        }

        Parameters additionalParameters = parseParameters( filename, validPrefix, validSuffix );
        return new ParseResult( Match.RECOGNIZED, additionalParameters );
    }

    private static Parameters parseParameters( String filename,
                                               String namePrefix,
                                               String extensionSuffix )
    {
        int additionalParamsStart = namePrefix.length();
        int additionalParamsEnd = filename.length() - extensionSuffix.length();
        String additionalParams = filename.substring( additionalParamsStart, additionalParamsEnd );

        if ( additionalParams.isEmpty() )
        {
            // filename contains no additional params
            return Parameters.NONE;
        }
        else
        {
            // Just a sanity check, to make sure additional parameters substring begins with correct separator
            if ( !additionalParams.startsWith( ADDITIONAL_PARAMS_SEPARATOR ) )
            {
                throw new RuntimeException( "Invalid additional parameters\n" +
                                            "Filename:          " + filename + "\n" +
                                            "Additional params: " + additionalParams );
            }

            // remove separator, so params can be parsed
            additionalParams = additionalParams.substring( ADDITIONAL_PARAMS_SEPARATOR.length() );
            return Parameters.parse( additionalParams );
        }
    }

    public static ProfilerRecordingDescriptor create( BenchmarkGroup benchmarkGroup,
                                                      Benchmark benchmark,
                                                      RunPhase runPhase,
                                                      ProfilerType profiler,
                                                      Parameters additionalParams )
    {
        List<RecordingType> secondaryRecordingTypes = new ArrayList<>( profiler.allRecordingTypes() );
        secondaryRecordingTypes.remove( profiler.recordingType() );
        return new ProfilerRecordingDescriptor( benchmarkGroup,
                                                benchmark,
                                                runPhase,
                                                profiler.recordingType(),
                                                secondaryRecordingTypes,
                                                additionalParams );
    }

    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final RecordingType recordingType;
    private final List<RecordingType> secondaryRecordingTypes;
    private final Parameters additionalParams;

    public ProfilerRecordingDescriptor( BenchmarkGroup benchmarkGroup,
                                        Benchmark benchmark,
                                        RunPhase runPhase,
                                        RecordingType recordingType,
                                        List<RecordingType> secondaryRecordingTypes,
                                        Parameters additionalParams )
    {
        this.benchmarkName = FullBenchmarkName.from( benchmarkGroup, benchmark );
        this.runPhase = runPhase;
        this.recordingType = recordingType;
        this.secondaryRecordingTypes = secondaryRecordingTypes;
        this.additionalParams = additionalParams;
    }

    public List<RecordingType> recordingTypes()
    {
        List<RecordingType> validRecordingTypes = new ArrayList<>( secondaryRecordingTypes );
        validRecordingTypes.add( recordingType );
        return validRecordingTypes;
    }

    public Parameters additionalParams()
    {
        return additionalParams;
    }

    public String sanitizedFilename()
    {
        return sanitizedFilename( recordingType );
    }

    public String sanitizedFilename( RecordingType recordingType )
    {
        return sanitize( filename( recordingType ) );
    }

    public String filename()
    {
        return filename( recordingType );
    }

    public String filename( RecordingType recordingType )
    {
        if ( !isSupportedRecordingType( recordingType ) )
        {
            throw new RuntimeException( "Invalid recording type: " + recordingType + "\n" +
                                        "Valid types: " + recordingTypes() );
        }
        return name() + recordingType.extension();
    }

    private boolean isSupportedRecordingType( RecordingType recordingType )
    {
        return this.recordingType == recordingType || secondaryRecordingTypes.contains( recordingType );
    }

    public String sanitizedName()
    {
        return sanitize( name() );
    }

    public String name()
    {
        return name( benchmarkName, runPhase, additionalParams );
    }

    private static String name( FullBenchmarkName benchmarkName, RunPhase runPhase, Parameters additionalParams )
    {
        String additionalParamsString = additionalParams.asMap().isEmpty()
                                        ? ""
                                        : ADDITIONAL_PARAMS_SEPARATOR + additionalParams.toString();
        return benchmarkName.name() + runPhase.nameModifier() + additionalParamsString;
    }

    @Override
    public String toString()
    {
        return String.format( "%s\n" +
                              "  %s\n" +
                              "  %s\n" +
                              "  %s:%s\n" +
                              "  %s",
                              getClass().getSimpleName(),
                              benchmarkName,
                              runPhase,
                              recordingType,
                              secondaryRecordingTypes,
                              additionalParams );
    }
}
