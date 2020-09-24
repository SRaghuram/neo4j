/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.neo4j.logging.Log;

import static com.neo4j.cc_robustness.util.Tarballs.targz;

class CcInstanceFiles
{
    private static final AtomicInteger zippedDbCounter = new AtomicInteger();
    private static final String INTERNAL_LOG_NAME = "debug.log";
    private final Path path;
    private final boolean keepHistory;
    private final Log log;

    CcInstanceFiles( Path path, Log log, boolean keepHistory )
    {
        this.path = path;
        this.log = log;
        this.keepHistory = keepHistory;
    }

    private static String zeroPad( int number, int size )
    {
        StringBuilder builder = new StringBuilder( String.valueOf( number ) );
        while ( builder.length() < size )
        {
            builder.insert( 0, "0" );
        }
        return builder.toString();
    }

    void init() throws IOException
    {
        System.out.println( "Running tests in: " + this.path.toAbsolutePath() );
        cleanDataDirectory( path );
        Files.createDirectories( path );
    }

    private Path dbsPath()
    {
        return path.resolve( "db" );
    }

    private Path packedDbFile( int serverId, int id )
    {
        return path.resolve( zeroPad( id, 3 ) + "_db" + serverId + ".tgz" );
    }

    synchronized void packDb( int serverId, boolean intermediary ) throws IOException
    {
        if ( SystemUtils.IS_OS_WINDOWS || intermediary && !keepHistory )
        {
            return;
        }

        Path dbPath = directoryFor( serverId );
        Path targetName = packedDbFile( serverId, zippedDbCounter.incrementAndGet() );

        targz( dbPath, targetName, log );

        removeOldPackedDbs( 10 );
    }

    Path directoryFor( int serverId )
    {
        return dbsPath().resolve( "" + serverId ).toAbsolutePath();
    }

    private void cleanDataDirectory( Path dir ) throws IOException
    {
        if ( Files.notExists( dir ) )
        {
            return;
        }
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( dir ) )
        {
            for ( Path child : paths )
            {
                // "output.txt" is the name of the file that the run bash-script uses to
                // redirect all outputs to.
                if ( Files.isRegularFile( child ) )
                {
                    if ( !child.getFileName().toString().equals( "output.txt" ) )
                    {
                        FileUtils.deleteQuietly( child.toFile() );
                    }
                }
                else
                {
                    FileUtils.deleteQuietly( child.toFile() );
                }
            }
        }
    }

    private void removeOldPackedDbs( int legRoom ) throws IOException
    {
        int tooOld = Math.max( 0, zippedDbCounter.get() - legRoom );
        try ( DirectoryStream<Path> paths = Files.newDirectoryStream( path ) )
        {
            for ( Path file : paths )
            {
                if ( file.getFileName().toString().contains( "_db" ) && idOfPackedFile( file ) <= tooOld )
                {
                    Files.delete( file );
                }
            }
        }
    }

    private int idOfPackedFile( Path file )
    {
        return Integer.parseInt( file.getFileName().toString().substring( 0, 3 ) );
    }

    void clean() throws IOException
    {
        // Removed packed dbs (every time they're shut down)
        removeOldPackedDbs( 0 );

        // Clean the database directories so that there are only messages.log left
        try ( Stream<Path> list = Files.list( dbsPath() ) )
        {
            list.filter( Files::isDirectory ).forEach( this::cleanDbDirectory );
        }
    }

    private void cleanDbDirectory( Path dbPath )
    {
        try ( Stream<Path> list = Files.list( dbPath ) )
        {
            list.filter( path -> !path.getFileName().toString().equals( INTERNAL_LOG_NAME ) ).forEach( path -> {
                try
                {
                    org.neo4j.io.fs.FileUtils.deleteDirectory( path );
                }
                catch ( IOException e )
                {
                    System.err.println( "Couldn't delete " + path + " while cleaning up after successful run, due to " + e );
                }
            } );
        }
        catch ( IOException e )
        {
            System.err.println( "Couldn't clean up after successful run, due to " + e );
        }
    }
}
