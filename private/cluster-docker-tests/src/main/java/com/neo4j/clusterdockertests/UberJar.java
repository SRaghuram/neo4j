/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private final File destination;
    private final boolean skipTestClasses;
    private FileOutputStream out;
    private JarOutputStream target;

    UberJar( File destination )
    {
        this( destination, true );
    }

    UberJar( File destination, boolean skipTestClasses )
    {
        this.destination = destination;
        this.skipTestClasses = skipTestClasses;
    }

    public void start() throws IOException
    {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put( Attributes.Name.MANIFEST_VERSION, "1.0" );

        this.out = new FileOutputStream( destination );
        this.target = new JarOutputStream( out, manifest );
    }

    public void addClass( File source )
    {
        throwIfNotAClass( source );

        String path = source.getPath();
        try
        {
            Pattern pattern = Pattern.compile( ".*[/\\\\]target[/\\\\](test-)?classes[/\\\\]" );
            MatchResult result = pattern.matcher( path ).results().collect( Collectors.toList() ).get( 0 );
            if ( skipTestClasses && result.group().replaceAll("\\\\", "/").endsWith( "/test-classes/" ) )
            {
                return;
            }

            Path root = Path.of( result.group() );

            File services = Path.of( root.toString(), "META-INF", "services" ).toFile();
            if ( services.exists() )
            {
                for ( File f : services.listFiles() )
                {
                    serviceDeclarations.putIfAbsent( f.getName(), new HashSet<>() );
                    serviceDeclarations.get( f.getName() ).addAll( Files.readAllLines( f.toPath() ) );
                }
            }
            String location = path.split( "[/\\\\]target[/\\\\](test-)?classes[/\\\\]" )[1];
            FileInputStream inputStream = new FileInputStream( source );

            writeJarEntry( source.lastModified(), target, location, inputStream );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void throwIfNotAClass( File source )
    {
        if ( !source.getName().endsWith( ".class" ) )
        {
            throw new IllegalArgumentException( "only class files are supported" );
        }
        if ( source.isDirectory() )
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
        final long time = Instant.now().toEpochMilli();
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

    private static void writeJarEntry( long time, JarOutputStream target, String location, InputStream inputStream ) throws IOException
    {
        JarEntry entry = new JarEntry( location.replace( "\\", "/" ) );
        entry.setTime( time );

        target.putNextEntry( entry );
        inputStream.transferTo( target );
        target.closeEntry();
    }
}
