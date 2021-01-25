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
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BenchmarkGroup
{
    public static class BenchmarkGroupKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String key, DeserializationContext ctxt )
        {
            return JsonUtil.deserializeJson( key, BenchmarkGroup.class );
        }
    }

    public static class BenchmarkGroupKeySerializer extends JsonSerializer<BenchmarkGroup>
    {
        @Override
        public void serialize( BenchmarkGroup value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
        {
            gen.writeFieldName( JsonUtil.serializeJson( value ) );
        }
    }

    public static final String NAME = "name";

    private final String name;

    @JsonCreator
    public BenchmarkGroup( @JsonProperty( "name" ) String name )
    {
        this.name = requireNonNull( name );
    }

    public String name()
    {
        return name;
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
        BenchmarkGroup that = (BenchmarkGroup) o;
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    @Override
    public String toString()
    {
        return "BenchmarkGroup{name='" + name + "'}";
    }
}
