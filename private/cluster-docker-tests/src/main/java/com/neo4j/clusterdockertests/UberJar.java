/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Creates a
 */
final class UberJar implements AutoCloseable
{
    private final Map<String,Set<String>> serviceDeclarations = new HashMap<>();
    private final Path destination;
    private final boolean skipTestClasses;
    private FileOutputStream out;
    private JarOutputStream target;

    UberJar( Path destination )
    {
        this( destination, true );
    }

    UberJar( Path destination, boolean skipTestClasses )
    {
        this.destination = destination;
        this.skipTestClasses = skipTestClasses;
    }

    public void start() throws IOException
    {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put( Attributes.Name.MANIFEST_VERSION, "1.0" );

        this.out = new FileOutputStream( destination.toFile() );
        this.target = new JarOutputStream( out, manifest );
    }

    public void addClass( Path source )
    {
        throwIfNotAClass( source );

        try
        {
            Pattern pattern = Pattern.compile( ".*[/\\\\]target[/\\\\](test-)?classes[/\\\\]" );
            MatchResult result = pattern.matcher( source.toString() ).results().collect( Collectors.toList() ).get( 0 );
            if ( skipTestClasses && result.group().replaceAll("\\\\", "/").endsWith( "/test-classes/" ) )
            {
                return;
            }

            Path root = Path.of( result.group() );

            Path services = Path.of( root.toString(), "META-INF", "services" );
            if ( Files.exists( services ) )
            {
                for ( Path f : Files.list( services ).collect( Collectors.toList() ) )
                {
                    String filename = f.getFileName().toString();
                    serviceDeclarations.putIfAbsent( filename, new HashSet<>() );
                    serviceDeclarations.get( filename ).addAll( Files.readAllLines( f ) );
                }
            }
            String location = source.toString().split( "[/\\\\]target[/\\\\](test-)?classes[/\\\\]" )[1];
            try ( FileInputStream inputStream = new FileInputStream( source.toFile() ) )
            {
                writeJarEntry( Files.getLastModifiedTime( source ), target, location, inputStream );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void throwIfNotAClass( Path source )
    {
        if ( !source.getFileName().toString().endsWith( ".class" ) )
        {
            throw new IllegalArgumentException( "only class files are supported" );
        }
        if ( Files.isDirectory( source ) )
        {
            throw new IllegalArgumentException( "A directory is not a class" );
        }
    }

    /**
     * Needed for SPI support
     *
     * @throws IOException
     */
    public void writeServiceDeclarations() throws IOException
    {
        final FileTime time = FileTime.from( Instant.now() );

        for ( var serviceDeclaration : serviceDeclarations.entrySet() )
        {
            String serviceName = serviceDeclaration.getKey();
            Set<String> serviceImplementations = serviceDeclaration.getValue();

            try ( ByteArrayInputStream inputStream = new ByteArrayInputStream( String.join( "\n", serviceImplementations ).getBytes() ) )
            {
                writeJarEntry( time, target, Path.of( "META-INF", "services", serviceName ).toString(), inputStream );
            }
        }
    }

    @Override
    public void close()
    {
        RuntimeException errors = new RuntimeException();
        if ( target != null )
        {
            try
            {
                target.close();
            }
            catch ( IOException e )
            {
                errors.addSuppressed( e );
            }
        }
        if ( out != null )
        {
            try
            {
                out.close();
            }
            catch ( IOException e )
            {
                errors.addSuppressed( e );
            }
        }
        if ( errors.getSuppressed().length > 0 )
        {
            throw errors;
        }
    }

    private static void writeJarEntry( FileTime time, JarOutputStream target, String location, InputStream inputStream ) throws IOException
    {
        JarEntry entry = new JarEntry( location.replace( "\\", "/" ) );
        entry.setTime( time.toMillis() );

        target.putNextEntry( entry );
        inputStream.transferTo( target );
        target.closeEntry();
    }
}
