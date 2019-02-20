package com.neo4j.bench.client.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.PropertyAccessor.ALL;
import static com.fasterxml.jackson.annotation.PropertyAccessor.FIELD;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class JsonUtil
{
    static class BenchmarkKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String s, DeserializationContext deserializationContext ) throws IOException
        {
            try
            {
                return OBJECT_MAPPER_HACKATRON
                        .readerFor( Benchmark.class )
                        .readValue( s.getBytes( StandardCharsets.UTF_8 ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    static class BenchmarkGroupKeyDeserializer extends KeyDeserializer
    {
        @Override
        public Object deserializeKey( String s, DeserializationContext deserializationContext ) throws IOException
        {
            try
            {
                return OBJECT_MAPPER_HACKATRON
                        .readerFor( BenchmarkGroup.class )
                        .readValue( s.getBytes( StandardCharsets.UTF_8 ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }

    private static final SimpleModule MODULE =
            new SimpleModule( "Benchmark Deserializer Module" )
                    .addKeyDeserializer( Benchmark.class, new BenchmarkKeyDeserializer() )
                    .addKeyDeserializer( BenchmarkGroup.class, new BenchmarkGroupKeyDeserializer() );

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable( SerializationFeature.FAIL_ON_EMPTY_BEANS )
            .setVisibility( ALL, NONE )
            .setVisibility( FIELD, ANY )
            .registerModule( MODULE )
            .enableDefaultTyping();

    private static final ObjectMapper OBJECT_MAPPER_HACKATRON = new ObjectMapper()
            .disable( SerializationFeature.FAIL_ON_EMPTY_BEANS )
            .setVisibility( ALL, NONE )
            .setVisibility( FIELD, ANY )
            .enableDefaultTyping();

    public static void serializeJson( Path file, Object object )
    {
        try ( BufferedWriter writer = Files.newBufferedWriter( file, UTF_8, CREATE, TRUNCATE_EXISTING ) )
        {
            OBJECT_MAPPER.writeValue( writer, object );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static String serializeJson( Object object )
    {
        try
        {
            return OBJECT_MAPPER.writeValueAsString( object );
        }
        catch ( JsonProcessingException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static <T> T deserializeJson( Path file, Class<T> c )
    {
        try ( BufferedReader reader = Files.newBufferedReader( file, UTF_8 ) )
        {
            return OBJECT_MAPPER.readerFor( c ).readValue( reader );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static <T> T deserializeJson( String json, Class<T> c )
    {
        try
        {
            return OBJECT_MAPPER.readerFor( c ).readValue( json.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
