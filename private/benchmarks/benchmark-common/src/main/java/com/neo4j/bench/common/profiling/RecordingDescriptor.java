/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.PathUtil;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.neo4j.bench.common.util.BenchmarkUtil.sanitize;

public class RecordingDescriptor
{

    private static final String PARAMS_EXTENSION = ".params";
    private static final PathUtil PATH_UTIL = PathUtil.withDefaultMaxLengthAndMargin( PARAMS_EXTENSION.length() );

    public static class RecordingDescriptorKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String key, DeserializationContext ctxt )
        {
            return JsonUtil.deserializeJson( key, RecordingDescriptor.class );
        }
    }

    public static class RecordingDescriptorKeySerializer extends JsonSerializer<RecordingDescriptor>
    {
        @Override
        public void serialize( RecordingDescriptor value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
        {
            gen.writeFieldName( JsonUtil.serializeJson( value ) );
        }
    }

    private static final String ADDITIONAL_PARAMS_SEPARATOR = ".";

    private final FullBenchmarkName benchmarkName;
    private final RunPhase runPhase;
    private final RecordingType recordingType;
    private final Parameters additionalParams;
    private final Set<FullBenchmarkName> secondaryBenchmarks;
    private final boolean isDuplicatesAllowed;

    @JsonCreator
    public RecordingDescriptor( @JsonProperty( "benchmarkName" ) FullBenchmarkName benchmarkName,
                                @JsonProperty( "runPhase" ) RunPhase runPhase,
                                @JsonProperty( "recordingType" ) RecordingType recordingType,
                                @JsonProperty( "additionalParams" ) Parameters additionalParams,
                                @JsonProperty( "secondaryBenchmarks" ) Set<FullBenchmarkName> secondaryBenchmarks,
                                @JsonProperty( "isDuplicatesAllowed" ) boolean isDuplicatesAllowed )
    {
        this.benchmarkName = benchmarkName;
        this.runPhase = runPhase;
        this.recordingType = recordingType;
        this.additionalParams = additionalParams;
        this.secondaryBenchmarks = secondaryBenchmarks;
        this.isDuplicatesAllowed = isDuplicatesAllowed;
    }

    public boolean isDuplicatesAllowed()
    {
        return isDuplicatesAllowed;
    }

    public FullBenchmarkName benchmarkName()
    {
        return benchmarkName;
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

    public String sanitizedName()
    {
        return sanitize( PATH_UTIL.limitLength( name() ) );
    }

    public String sanitizedFilename()
    {
        return sanitizedFilename( recordingType.extension() );
    }

    public String sanitizedFilename( String extension )
    {
        return sanitizedFilename( "", extension );
    }

    public String sanitizedFilename( String prefix, String extension )
    {
        return sanitize( filename( prefix, extension ) );
    }

    public String paramsFilename()
    {
        return filename() + PARAMS_EXTENSION;
    }

    public String filename()
    {
        return filename( "", recordingType.extension() );
    }

    private String filename( String prefix, String extension )
    {
        return PATH_UTIL.limitLength( prefix, name(), extension );
    }

    public String name()
    {
        String additionalParamsString = additionalParams.asMap().isEmpty()
                                        ? ""
                                        : ADDITIONAL_PARAMS_SEPARATOR + additionalParams.toString();
        return benchmarkName.name() + runPhase.nameModifier() + additionalParamsString;
    }

    public Set<RecordingDescriptor> secondaryRecordingDescriptors()
    {
        return secondaryBenchmarks.stream()
                                  .map( this::copySecondaryRecordingDescriptor )
                                  .collect( Collectors.toSet() );
    }

    private RecordingDescriptor copySecondaryRecordingDescriptor( FullBenchmarkName secondaryBenchmark )
    {
        return new RecordingDescriptor( secondaryBenchmark, runPhase, recordingType, additionalParams, Collections.emptySet(), false );
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
