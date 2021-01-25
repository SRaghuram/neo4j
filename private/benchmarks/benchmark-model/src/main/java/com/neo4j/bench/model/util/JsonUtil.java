/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable( SerializationFeature.FAIL_ON_EMPTY_BEANS )
            .setVisibility( ALL, NONE )
            .setVisibility( FIELD, ANY )
            .registerModule( new JavaTimeModule() )
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
