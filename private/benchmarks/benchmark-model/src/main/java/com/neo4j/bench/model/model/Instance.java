/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.IOException;
import java.util.Map;

public class Instance
{

    public static class InstanceKeySerializer extends JsonSerializer<Instance>
    {
        @Override
        public void serialize( Instance value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
        {
            gen.writeFieldName( JsonUtil.serializeJson( value ) );
        }
    }

    public static class InstanceKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String key, DeserializationContext ctxt )
        {
            return JsonUtil.deserializeJson( key, Instance.class );
        }
    }

    public static Instance server( String serverName, String operatingSystem, int availableProcessors, long totalMemory )
    {
        return new Instance( serverName, Kind.Server, operatingSystem, availableProcessors, totalMemory );
    }

    public static Instance aws( String instanceType, String operatingSystem, int availableProcessors, long totalMemory )
    {
        return new Instance( instanceType, Kind.AWS, operatingSystem, availableProcessors, totalMemory );
    }

    public enum Kind
    {
        Server,
        AWS
    }

    private final String host;
    private final Kind kind;
    private final String operatingSystem;
    private final int availableProcessors;
    private final long totalMemory;

    /**
     * @param host host on which we run benchmark, it can be its hostname (when Kind.Server) or instance type (when kind.AWS)
     * @param kind instance kind, bare metal server, AWS EC2, etc.
     * @param operatingSystem operating system
     * @param availableProcessors number of available processors
     * @param totalMemory total memory in MiB
     */
    @JsonCreator
    public Instance( @JsonProperty( "host" ) String host,
                     @JsonProperty( "kind" ) Kind kind,
                     @JsonProperty( "operatingSystem" ) String operatingSystem,
                     @JsonProperty( "availableProcessors" ) int availableProcessors,
                     @JsonProperty( "totalMemory" ) long totalMemory )
    {
        this.host = host;
        this.kind = kind;
        this.operatingSystem = operatingSystem;
        this.availableProcessors = availableProcessors;
        this.totalMemory = totalMemory;
    }

    public String host()
    {
        return host;
    }

    public Kind kind()
    {
        return kind;
    }

    public String operatingSystem()
    {
        return operatingSystem;
    }

    public int availableProcessors()
    {
        return availableProcessors;
    }

    public long totalMemory()
    {
        return totalMemory;
    }

    public Map<String,Object> toMap()
    {
        return ImmutableMap.<String,Object>builder()
                .put( "host", host )
                .put( "kind", kind.name() )
                .put( "availableCores", availableProcessors )
                .put( "totalMemory", totalMemory )
                .put( "operatingSystem", operatingSystem )
                .build();
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
        return ToStringBuilder.reflectionToString( this );
    }
}
