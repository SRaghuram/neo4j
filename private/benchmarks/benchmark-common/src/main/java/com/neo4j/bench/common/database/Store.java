/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import com.neo4j.bench.common.util.BenchmarkUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.neo4j.bench.common.util.BenchmarkUtil.deleteDir;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Store implements AutoCloseable
{
    // Copy from GraphDatabaseSettings
    private static final String SYSTEM_DATABASE_NAME = "system";
    private final Path topLevelDir;
    private final Path graphDbDir;
    private final boolean isTemporaryCopy;

    public static void assertDirectoryIsNeoStore( Path topLevelDir )
    {
        discoverGraphDbOrFail( topLevelDir );
    }

    /**
     * Store directory will not be deleted on close, must be deleted manually by caller.
     *
     * @param originalTopLevelDir top level directory of Neo4j store, must contain a graph.db/ directory
     * @return new Store
     */
    public static Store createFrom( Path originalTopLevelDir )
    {
        return new Store( originalTopLevelDir, false );
    }

    /**
     * Store directory will be automatically deleted on close.
     *
     * @return new temporary Store
     */
    public Store makeTemporaryCopy()
    {
        Path copyParent = topLevelDir.getParent();
        Path storeCopyPath = storeCopyPath( topLevelDir, copyParent );
        System.out.println( "Making temporary store copy..." );
        copy( topLevelDir, storeCopyPath );
        return new Store( storeCopyPath, true );
    }

    private Store( Path topLevelDir, boolean isTemporaryCopy )
    {
        this.topLevelDir = topLevelDir;
        this.isTemporaryCopy = isTemporaryCopy;
        BenchmarkUtil.assertDirectoryExists( topLevelDir );
        this.graphDbDir = discoverGraphDbOrFail( topLevelDir );
    }

    private static Path discoverGraphDbOrFail( Path topLevelDir )
    {
        List<Path> paths = discoverGraphDbs( topLevelDir );
        if ( paths.size() == 0 )
        {
            throw new RuntimeException( "Could not find any store in: " + topLevelDir );
        }
        if ( paths.size() > 1 )
        {
            String pathNames = paths.stream().map( Path::toString ).collect( Collectors.joining( "\n" ) );
            throw new RuntimeException( "Found more than one store in: " + topLevelDir + "\n" + pathNames );
        }
        return paths.get( 0 );
    }

    private static List<Path> discoverGraphDbs( Path topLevelDir )
    {
        try
        {
            return Files.walk( topLevelDir )
                        .filter( Files::isDirectory )
                        .filter( Store::isGraphDb )
                        .filter( path -> !path.endsWith( SYSTEM_DATABASE_NAME ) )
                        .collect( Collectors.toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static boolean isGraphDb( Path maybeGraphDb )
    {
        try
        {
            return Files.list( maybeGraphDb )
                        .anyMatch( file -> file.getFileName().startsWith( "neostore" ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public Store makeCopyAt( Path topLevelDirCopy )
    {
        BenchmarkUtil.assertDoesNotExist( topLevelDirCopy );
        // we need to make sure topLevelDirCopy is not relative path
        BenchmarkUtil.assertDirectoryExists( topLevelDirCopy.toAbsolutePath().getParent() );
        copy( topLevelDir, topLevelDirCopy );
        return Store.createFrom( topLevelDirCopy );
    }

    public Path topLevelDirectory()
    {
        return topLevelDir;
    }

    public Path graphDbDirectory()
    {
        return graphDbDir;
    }

    public long bytes()
    {
        return BenchmarkUtil.bytes( topLevelDir );
    }

    public void removeIndexDir()
    {
        Path indexDir = graphDbDirectory().resolve( "schema/" );
        if ( Files.exists( indexDir ) )
        {
            deleteDir( indexDir );
        }
    }

    public void removeTxLogs()
    {
        for ( Path txLog : getTxLogs() )
        {
            try
            {
                Files.delete( txLog );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Error deleting TX log: " + txLog.toAbsolutePath(), e );
            }
        }
    }

    private List<Path> getTxLogs()
    {
        try ( Stream<Path> entries = Files.list( graphDbDirectory() ) )
        {
            return entries
                    .filter( p -> p.toString().startsWith( "neostore.transaction.db." ) )
                    .collect( toList() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public void delete()
    {
        deleteDir( topLevelDir );
    }

    @Override
    public String toString()
    {
        return "Neo4j Store\n" +
               "\tPath : " + topLevelDir.toAbsolutePath() + "\n" +
               "\tSize : " + BenchmarkUtil.bytesToString( bytes() );
    }

    @Override
    public void close()
    {
        if ( isTemporaryCopy )
        {
            System.out.println( "Deleting store: " + topLevelDir.toAbsolutePath() );

            BenchmarkUtil.assertDirectoryExists( topLevelDir );
            delete();
        }
    }

    private static void copy( Path from, Path to )
    {
        System.out.println( format( "Copying store...\n" +
                                    "From : %s\n" +
                                    "To   : %s",
                                    from.toAbsolutePath(), to.toAbsolutePath() ) );
        try
        {
            CopyDirVisitor visitor = new CopyDirVisitor( from, to );
            Files.walkFileTree( from, visitor );
            visitor.awaitCompletion();
        }
        catch ( Exception e )
        {
            IOException ioe;
            if ( e instanceof IOException )
            {
                ioe = (IOException) e;
            }
            else
            {
                ioe = new IOException( e );
            }
            throw new UncheckedIOException( format( "Error copying DB from %s to %s", from, to ), ioe );
        }
    }

    private static Path storeCopyPath( Path originalStore, Path copyParent )
    {
        return copyParent.resolve( nameOf( originalStore ) + "-copy-" + UUID.randomUUID().toString() );
    }

    private static String nameOf( Path path )
    {
        return path.getFileName().toString();
    }

    private static class CopyDirVisitor extends SimpleFileVisitor<Path>
    {
        private final ExecutorService executorService;
        private final List<Future<Void>> copyingProcesses;
        private final Path fromPath;
        private final Path toPath;

        private CopyDirVisitor( Path fromPath, Path toPath )
        {
            this.fromPath = fromPath;
            this.toPath = toPath;
            executorService = Executors.newFixedThreadPool( 6 );
            copyingProcesses = new ArrayList<>();
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) throws IOException
        {
            Path targetPath = toPath.resolve( fromPath.relativize( dir ) );
            if ( !Files.exists( targetPath ) )
            {
                Files.createDirectory( targetPath );
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
        {
            copyingProcesses.add( executorService.submit( () ->
                                                          {
                                                              Files.copy( file, toPath.resolve( fromPath.relativize( file ) ) );
                                                              return null;
                                                          } ) );
            return FileVisitResult.CONTINUE;
        }

        private void awaitCompletion() throws Exception
        {
            for ( Future<Void> copyingProcess : copyingProcesses )
            {
                copyingProcess.get();
            }
            executorService.shutdown();
        }
    }
}
