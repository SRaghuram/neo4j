/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import com.google.common.io.CharStreams;
import com.neo4j.bench.model.util.JsonUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;

public class Resources implements AutoCloseable
{
    public static String fileToString( String filename )
    {
        try ( InputStream inputStream = Resources.class.getResource( filename ).openStream() )
        {
            return CharStreams.toString( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error reading stream to string", e );
        }
    }

    private static final String DUMP_DIR_NAME = "resources_copy";

    private final Path workDir;
    private final ResourcesSubscriptions subscriptions;
    private final Path dumpDir;

    public Resources( Path workDir )
    {
        this.workDir = workDir;
        BenchmarkUtil.assertDirectoryExists( workDir );
        this.dumpDir = workDir.resolve( DUMP_DIR_NAME );
        if ( !Files.exists( dumpDir ) )
        {
            try
            {
                Files.createDirectory( dumpDir );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Error while trying to create: " + dumpDir.toAbsolutePath(), e );
            }
        }
        this.subscriptions = ResourcesSubscriptions.load( dumpDir );
        this.subscriptions.inc();
    }

    public Path workDir()
    {
        return workDir;
    }

    public Path getResourceFile( String resourceFilename )
    {
        assertStartsWithSeparatorChar( resourceFilename );
        String topLevelResourceFilename = toTopLevelResourceFilename( resourceFilename );
        // eagerly load all files in top level folder of this file
        innerGet( topLevelResourceFilename );
        // return requested resource file
        return innerGet( resourceFilename );
    }

    private static String toTopLevelResourceFilename( String resourceFilename )
    {
        return "/" + resourceFilename.substring( 1 ).split( "/" )[0];
    }

    private Path innerGet( String resourceFilename )
    {
        URI resourceUri = resourceUriFor( resourceFilename );
        if ( resourceUri.toString().contains( ".jar!" ) )
        {
            try ( FileSystem fileSystem = createFileSystem( resourceUri ) )
            {
                return getOrMakeCopy( fileSystem.getPath( resourceFilename ), resourceFilename );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Failed to copy file from resources: " + resourceFilename, e );
            }
        }
        return getOrMakeCopy( Paths.get( resourceUri ), resourceFilename );
    }

    private static URI resourceUriFor( String filename )
    {
        try
        {
            return Resources.class.getResource( filename ).toURI();
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( "Error retrieve URI for resource with name: " + filename );
        }
    }

    private static FileSystem createFileSystem( URI uri )
    {
        try
        {
            return FileSystems.newFileSystem( uri, Collections.emptyMap() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to initialize file system for URI: " + uri, e );
        }
    }

    private Path getOrMakeCopy( Path originalResourceFile, String resourceFilename )
    {
        assertStartsWithSeparatorChar( resourceFilename );
        resourceFilename = resourceFilename.substring( 1 );
        Path resourceCopyPath = dumpDir.resolve( resourceFilename );
        if ( Files.exists( resourceCopyPath ) )
        {
            return resourceCopyPath;
        }
        else
        {
            try
            {
                Files.walkFileTree( originalResourceFile, new CopyDirVisitor( originalResourceFile, resourceCopyPath ) );
                return resourceCopyPath;
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Error trying to copy resources file to temp directory\n" +
                                                "From : " + originalResourceFile.toAbsolutePath() + "\n" +
                                                "To   : " + resourceCopyPath.toAbsolutePath(), e );
            }
        }
    }

    private static void assertStartsWithSeparatorChar( String resourceFilename )
    {
        // resource paths should always start with '/'
        if ( !resourceFilename.startsWith( "/" ) )
        {
            throw new RuntimeException( "Resources path unexpectedly did not start with '/'" );
        }
    }

    @Override
    public void close()
    {
        subscriptions.dec();
        if ( subscriptions.hasSubscribers() )
        {
            if ( Files.exists( dumpDir ) )
            {
                BenchmarkUtil.deleteDir( dumpDir );
            }
        }
    }

    private static class CopyDirVisitor extends SimpleFileVisitor<Path>
    {
        private final Path fromPath;
        private final Path toPath;

        private CopyDirVisitor( Path fromPath, Path toPath )
        {
            this.fromPath = pathTransform( toPath.getFileSystem(), fromPath );
            this.toPath = toPath;
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) throws IOException
        {
            Path transformedDir = pathTransform( toPath.getFileSystem(), dir );
            Path targetPath = toPath.resolve( fromPath.relativize( transformedDir ) );

            Files.createDirectories( targetPath );
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path fromFile, BasicFileAttributes attrs ) throws IOException
        {
            Path transformedFromFile = pathTransform( toPath.getFileSystem(), fromFile );
            Path toFile = toPath.resolve( fromPath.relativize( transformedFromFile ) );

            if ( !Files.exists( toFile ) )
            {
                Files.copy( fromFile, toFile );
            }
            return FileVisitResult.CONTINUE;
        }

        /**
         * Transforms given path, into a path for the local file system.
         * The given path may be inside of a .jar file, which much be read using a different file system.
         * This transformation is necessary because many {@link Path} methods do not work on paths that are not from the same file system.
         *
         * @return path, transformed to the local filesystem
         */
        private static Path pathTransform( FileSystem localFileSystem, Path pathToTransform )
        {
            Path transformedPath = localFileSystem.getPath( pathToTransform.isAbsolute() ? localFileSystem.getSeparator() : "" );
            for ( Path pathComponent : pathToTransform )
            {
                transformedPath = transformedPath.resolve( pathComponent.getFileName().toString() );
            }
            return transformedPath;
        }
    }

    private static class ResourcesSubscriptions
    {
        private static final String RESOURCES_SUBSCRIPTIONS_JSON = "resources-subscriptions.json";

        private static ResourcesSubscriptions load( Path dir )
        {
            Path jsonPath = dir.resolve( RESOURCES_SUBSCRIPTIONS_JSON );
            if ( !Files.exists( jsonPath ) )
            {
                try
                {
                    Files.createFile( jsonPath );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( "Failed to create: " + jsonPath.toAbsolutePath(), e );
                }
                ResourcesSubscriptions subscriptions = new ResourcesSubscriptions( jsonPath, 0 );
                JsonUtil.serializeJson( jsonPath, subscriptions );
                return subscriptions;
            }
            else
            {
                return JsonUtil.deserializeJson( jsonPath, ResourcesSubscriptions.class );
            }
        }

        private final Path jsonPath;
        private Integer subscribers;

        /**
         * WARNING: Never call this explicitly.
         * No-params constructor is only used for JSON (de)serialization.
         */
        private ResourcesSubscriptions()
        {
            this( null, 0 );
        }

        private ResourcesSubscriptions( Path jsonPath, Integer subscribers )
        {
            this.jsonPath = jsonPath;
            this.subscribers = subscribers;
        }

        private void inc()
        {
            subscribers++;
            JsonUtil.serializeJson( jsonPath, this );
        }

        private void dec()
        {
            subscribers--;
            JsonUtil.serializeJson( jsonPath, this );
        }

        private boolean hasSubscribers()
        {
            return subscribers == 0;
        }
    }
}
