/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;

public class RecordingDescriptor
{
    public static class RecordingDescriptorKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String key, DeserializationContext ctxt )
        {
            return JsonUtil.deserializeJson( key, RecordingDescriptor.class );
        }
    }

    private static final String ADDITIONAL_PARAMS_SEPARATOR = ".";

    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final RecordingType recordingType;
    private final Parameters additionalParams;

    @JsonCreator
    public RecordingDescriptor( @JsonProperty( "benchmarkName" ) FullBenchmarkName benchmarkName,
                                @JsonProperty( "runPhase" ) RunPhase runPhase,
                                @JsonProperty( "recordingType" ) RecordingType recordingType,
                                @JsonProperty( "additionalParams" ) Parameters additionalParams )
    {
        this.benchmarkName = benchmarkName;
        this.runPhase = runPhase;
        this.recordingType = recordingType;
        this.additionalParams = additionalParams;
    }

    public RunPhase runPhase()
    {
        return runPhase;
    }

    public RecordingType recordingType()
    {
        return recordingType;
    }

    public Parameters additionalParams()
    {
        return additionalParams;
    }

    public String sanitizedFilename()
    {
        return sanitize( filename() );
    }

    public String filename()
    {
        return name() + recordingType.extension();
    }

    public String sanitizedName()
    {
        return sanitize( name() );
    }

    public String name()
    {
        String additionalParamsString = additionalParams.asMap().isEmpty()
                                        ? ""
                                        : ADDITIONAL_PARAMS_SEPARATOR + additionalParams.toString();
        return benchmarkName.name() + runPhase.nameModifier() + additionalParamsString;
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return String.format( "%s\n" +
                              "  %s\n" +
                              "  %s\n" +
                              "  %s\n" +
                              "  %s",
                              getClass().getSimpleName(),
                              benchmarkName,
                              runPhase,
                              recordingType,
                              additionalParams );
    }
}
