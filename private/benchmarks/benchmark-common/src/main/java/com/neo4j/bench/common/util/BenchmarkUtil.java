/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class BenchmarkUtil
{
    public interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    public static <EXCEPTION extends Throwable> EXCEPTION assertException( Class<EXCEPTION> exception, ThrowingRunnable fun )
    {
        try
        {
            fun.run();
        }
        catch ( Throwable e )
        {
            if ( e.getClass().equals( exception ) )
            {
                return (EXCEPTION) e;
            }
            else
            {
                throw new RuntimeException( format( "Expected exception of type %s but was %s", exception.getName(), e.getClass().getName() ), e );
            }
        }
        throw new RuntimeException( "Expected exception to be thrown: " + exception.getName() );
    }

    public static String generateUniqueId()
    {
        return UUID.randomUUID().toString();
    }

    public static Path getPathEnvironmentVariable( String envVar )
    {
        String pathString = System.getenv( envVar );
        if ( null == pathString )
        {
            throw new RuntimeException( "Environment variable not set: " + envVar );
        }
        else
        {
            return Paths.get( pathString );
        }
    }

    public static String sanitize( String string )
    {
        return lessWhiteSpace(
                string.trim()
                      .replace( ",", "_" )
                      .replace( "(", "_" )
                      .replace( ")", "_" )
                      .replace( "-", " " )
                      .replace( "|", "_" ) )
                .replace( " ", "_" );
    }

    public static void stringToFile( String contents, Path outputFile )
    {
        try
        {
            BenchmarkUtil.forceRecreateFile( outputFile );
            Files.write( outputFile, contents.getBytes( StandardCharsets.UTF_8 ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to write map to file: " + outputFile.toAbsolutePath(), e );
        }
    }

    public static Map<String,String> propertiesPathToMap( Path propertiesFile )
    {
        try ( InputStream inputStream = Files.newInputStream( propertiesFile ) )
        {
            return new HashMap<>( Maps.fromProperties( toProperties( inputStream ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static Properties toProperties( InputStream inputStream )
    {
        try
        {
            Properties properties = new Properties();
            properties.load( inputStream );
            return properties;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error reading stream to properties", e );
        }
    }

    public static void deleteDir( Path dir )
    {
        try
        {
            assertDirectoryExists( dir );

            try ( Stream<Path> paths = Files.walk( dir ) )
            {
                paths
                        .sorted( Comparator.reverseOrder() )
                        .map( Path::toFile )
                        .forEach( File::delete );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error encountered while trying to delete directory: " + dir.toAbsolutePath(), e );
        }
    }

    public static Path forceRecreateFile( Path path )
    {
        try
        {
            if ( Files.exists( path ) )
            {
                assertNotDirectory( path );
                Files.delete( path );
            }
            return Files.createFile( path );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Path: %s", path ), e );
        }
    }

    public static Path tryMkDir( Path path )
    {
        try
        {
            if ( !Files.exists( path ) )
            {
                return Files.createDirectories( path );
            }
            else if ( !Files.isDirectory( path ) )
            {
                throw new RuntimeException( "Path is not a directory: " + path.toAbsolutePath() );
            }
            else
            {
                return path;
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to create directory: " + path.toAbsolutePath(), e );
        }
    }

    public static Set<String> splitAndTrimCommaSeparatedString( String value )
    {
        return Stream.of( value.split( "," ) )
                     .map( String::trim )
                     .filter( s -> !s.isEmpty() )
                     .collect( toSet() );
    }

    public static void assertDoesNotExist( Path file )
    {
        if ( Files.exists( file ) )
        {
            throw new RuntimeException( "File already exists: " + file.toAbsolutePath() );
        }
    }

    public static void assertDirectoryExists( Path dir )
    {
        if ( !Files.exists( dir ) )
        {
            throw new RuntimeException( "Directory does not exist: " + dir.toAbsolutePath() );
        }
        else
        {
            assertIsDirectory( dir );
        }
    }

    public static void assertFileExists( Path file )
    {
        if ( !Files.exists( file ) )
        {
            throw new RuntimeException( "File does not exist: " + file.toAbsolutePath() );
        }
        else
        {
            assertNotDirectory( file );
        }
    }

    public static void assertIsDirectory( Path dir )
    {
        if ( !Files.isDirectory( dir ) )
        {
            throw new RuntimeException( "Is not a directory: " + dir.toAbsolutePath() );
        }
    }

    public static void assertNotDirectory( Path path )
    {
        if ( Files.isDirectory( path ) )
        {
            throw new RuntimeException( "Is a directory, not a file: " + path.toAbsolutePath() );
        }
    }

    public static void assertFileNotEmpty( Path file )
    {
        assertFileExists( file );
        try
        {
            if ( !Files.lines( file ).findFirst().isPresent() )
            {
                throw new RuntimeException( "File is empty: " + file.toAbsolutePath() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Cannot open file: " + file.toAbsolutePath(), e );
        }
    }

    public static String durationToString( Duration duration )
    {
        long durationAsNano = duration.toNanos();
        long h = NANOSECONDS.toHours( durationAsNano );
        long m = NANOSECONDS.toMinutes( durationAsNano ) -
                 HOURS.toMinutes( h );
        long s = NANOSECONDS.toSeconds( durationAsNano ) -
                 HOURS.toSeconds( h ) -
                 MINUTES.toSeconds( m );
        long ms = NANOSECONDS.toMillis( durationAsNano ) -
                  HOURS.toMillis( h ) -
                  MINUTES.toMillis( m ) -
                  SECONDS.toMillis( s );
        long us = NANOSECONDS.toMicros( durationAsNano ) -
                  HOURS.toMicros( h ) -
                  MINUTES.toMicros( m ) -
                  SECONDS.toMicros( s ) -
                  MILLISECONDS.toMicros( ms );
        long ns = NANOSECONDS.toMicros( durationAsNano ) -
                  HOURS.toMicros( h ) -
                  MINUTES.toMicros( m ) -
                  SECONDS.toMicros( s ) -
                  MILLISECONDS.toMicros( ms ) -
                  MILLISECONDS.toNanos( us );
        if ( ns > 0 )
        {
            return durationToString( h, m, s, ms, us, ns );
        }
        else if ( us > 0 )
        {
            return durationToString( h, m, s, ms, us );
        }
        else if ( ms > 0 )
        {
            return durationToString( h, m, s, ms );
        }
        else
        {
            return durationToString( h, m, s );
        }
    }

    private static String durationToString( long h, long m, long s )
    {
        return format( "%02d:%02d:%02d", h, m, s );
    }

    private static String durationToString( long h, long m, long s, long ms )
    {
        return format( "%02d:%02d:%02d.%03d", h, m, s, ms );
    }

    private static String durationToString( long h, long m, long s, long ms, long us )
    {
        return format( "%02d:%02d:%02d.%03d.%03d", h, m, s, ms, us );
    }

    private static String durationToString( long h, long m, long s, long ms, long us, long ns )
    {
        return format( "%02d:%02d:%02d.%03d.%03d.%03d", h, m, s, ms, us, ns );
    }

    public static String fileToString( Path file )
    {
        return inputStreamToString( file );
    }

    private static String inputStreamToString( Path file )
    {
        try ( InputStream inputStream = Files.newInputStream( file ) )
        {
            return CharStreams.toString( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error reading stream to string", e );
        }
    }

    public static void appendFile( Path file, Instant timeStamp, String... lines )
    {
        try ( PrintWriter printer = new PrintWriter( Files.newOutputStream( file, StandardOpenOption.APPEND ), true /*auto flush*/ ) )
        {
            Arrays.stream( lines ).forEach( line -> printer.println( "[" + timeStamp + "] " + line ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error writing line to file\n" +
                                        "Line: " + Arrays.toString( lines ) + "\n" +
                                        "File: " + file.toAbsolutePath(), e );
        }
    }

    public static String inputStreamToString( InputStream is ) throws IOException
    {
        return String.join( "\n", inputStreamToLines( is ) );
    }

    public static List<String> inputStreamToLines( InputStream is ) throws IOException
    {
        try ( BufferedReader buffer = new BufferedReader( new InputStreamReader( is ) ) )
        {
            return buffer.lines().collect( toList() );
        }
    }

    public static String lessWhiteSpace( String string )
    {
        return string.trim().replaceAll( "\\s+", " " );
    }

    public static String bytesToString( long bytes )
    {
        DecimalFormat format = new DecimalFormat( "###,###,###,###,###,###,##0" );
        long kb = bytes / 1024;
        long mb = kb / 1024;
        long gb = mb / 1024;
        if ( gb > 0 )
        {
            return format( "%s (gb)", format.format( gb ) );
        }
        else if ( mb > 0 )
        {
            return format( "%s (mb)", format.format( mb ) );
        }
        else if ( kb > 0 )
        {
            return format( "%s (kb)", format.format( kb ) );
        }
        else
        {
            return format( "%s (byte)", format.format( bytes ) );
        }
    }

    public static long bytes( Path dir )
    {
        if ( !Files.exists( dir ) )
        {
            return 0;
        }
        try ( Stream<Path> paths = Files.list( dir ) )
        {
            long bytes = 0;
            for ( Path path : paths.collect( toList() ) )
            {
                if ( path.toFile().isFile() )
                {
                    bytes += path.toFile().length();
                }
                else
                {
                    bytes += bytes( path );
                }
            }
            return bytes;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
