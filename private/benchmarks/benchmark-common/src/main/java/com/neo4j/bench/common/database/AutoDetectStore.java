/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import com.neo4j.bench.common.util.BenchmarkUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class AutoDetectStore extends Store
{
    // Copy from GraphDatabaseSettings
    private static final String SYSTEM_DATABASE_NAME = "system";
    private final Path topLevelDir;
    private final Path graphDbDir;
    private final boolean isTemporaryCopy;

    /**
     * Store directory will not be deleted on close, must be deleted manually by caller.
     *
     * @param originalTopLevelDir top level directory of Neo4j store, must contain a graph.db/ directory
     * @return new Store
     */
    public static AutoDetectStore createFrom( Path originalTopLevelDir )
    {
        return new AutoDetectStore( originalTopLevelDir, false );
    }

    /**
     * Store directory will be automatically deleted on close.
     *
     * @return new temporary Store
     */
    @Override
    public AutoDetectStore makeTemporaryCopy()
    {
        return new AutoDetectStore( StoreUtils.makeCopy( topLevelDir ), true );
    }

    private AutoDetectStore( Path topLevelDir, boolean isTemporaryCopy )
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
        try ( Stream<Path> files = Files.walk( topLevelDir ) )
        {
            return files.filter( Files::isDirectory )
                        .filter( AutoDetectStore::isGraphDb )
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
                    .anyMatch( file ->
                    {
                        String fileName = file.getFileName().toString();
                        boolean storefile = fileName.startsWith( "neostore" ) || fileName.startsWith( "main-store" );
                        storefile &= !fileName.contains( "transaction" );
                        storefile &= !fileName.contains( "cacheprof" );
                        return storefile;
                    } );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public AutoDetectStore makeCopyAt( Path topLevelDirCopy )
    {
        BenchmarkUtil.assertDoesNotExist( topLevelDirCopy );
        // we need to make sure topLevelDirCopy is not relative path
        BenchmarkUtil.assertDirectoryExists( topLevelDirCopy.toAbsolutePath().getParent() );
        StoreUtils.copy( topLevelDir, topLevelDirCopy );
        return AutoDetectStore.createFrom( topLevelDirCopy );
    }

    @Override
    public void assertDirectoryIsNeoStore()
    {
        discoverGraphDbOrFail( topLevelDir );
    }

    @Override
    public Path topLevelDirectory()
    {
        return topLevelDir;
    }

    @Override
    public Path graphDbDirectory()
    {
        return graphDbDir;
    }

    @Override
    public DatabaseName databaseName()
    {
        return new DatabaseName( graphDbDirectory().getFileName().toString() );
    }

    @Override
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

    @Override
    boolean isTemporaryCopy()
    {
        return isTemporaryCopy;
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

    @Override
    public String toString()
    {
        return "AutoDetectStore\n" +
               "\tPath : " + topLevelDir.toAbsolutePath() + "\n" +
               "\tSize : " + BenchmarkUtil.bytesToString( bytes() );
    }
}
