/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.logging.Log;

import static com.neo4j.cc_robustness.util.Tarballs.targz;

class CcInstanceFiles
{
    private static final AtomicInteger zippedDbCounter = new AtomicInteger();
    private static final String INTERNAL_LOG_NAME = "debug.log";
    private final File path;
    private final boolean keepHistory;
    private final Log log;

    CcInstanceFiles( File path, Log log, boolean keepHistory )
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

    void init()
    {
        System.out.println( "Running tests in: " + this.path.getAbsolutePath() );
        cleanDataDirectory( path );
        defaultDiscoveryFile().delete();
        path.mkdirs();
    }

    private File dbsPath()
    {
        return new File( path, "db" );
    }

    private File packedDbFile( int serverId, int id )
    {
        return new File( path, zeroPad( id, 3 ) + "_db" + serverId + ".tgz" );
    }

    synchronized void packDb( int serverId, boolean intermediary )
    {
        if ( SystemUtils.IS_OS_WINDOWS || intermediary && !keepHistory )
        {
            return;
        }

        File dbPath = directoryFor( serverId );
        File targetName = packedDbFile( serverId, zippedDbCounter.incrementAndGet() );

        targz( dbPath, targetName, log );

        removeOldPackedDbs( 10 );
    }

    File directoryFor( int serverId )
    {
        return new File( dbsPath(), "" + serverId ).getAbsoluteFile();
    }

    private void cleanDataDirectory( File dir )
    {
        if ( !dir.exists() )
        {
            return;
        }
        for ( File child : dir.listFiles() )
        {
            // "output.txt" is the name of the file that the run bash-script uses to
            // redirect all outputs to.
            if ( child.isFile() )
            {
                if ( !child.getName().equals( "output.txt" ) )
                {
                    child.delete();
                }
            }
            else
            {
                FileUtils.deleteQuietly( child );
            }
        }
    }

    private File defaultDiscoveryFile()
    {
        try
        {
            File file = File.createTempFile( "cronie", "test" );
            file.delete();
            return new File( file.getParentFile(), "cronie-discovery" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void removeOldPackedDbs( int legRoom )
    {
        int tooOld = Math.max( 0, zippedDbCounter.get() - legRoom );
        for ( File file : path.listFiles() )
        {
            if ( file.getName().contains( "_db" ) && idOfPackedFile( file ) <= tooOld )
            {
                file.delete();
            }
        }
    }

    private int idOfPackedFile( File file )
    {
        return Integer.parseInt( file.getName().substring( 0, 3 ) );
    }

    void clean()
    {
        // Removed packed dbs (every time they're shut down)
        removeOldPackedDbs( 0 );

        // Clean the database directories so that there are only messages.log left
        for ( File dbPath : dbsPath().listFiles() )
        {
            if ( dbPath.isDirectory() )
            {
                cleanDbDirectory( dbPath );
            }
        }
    }

    private void cleanDbDirectory( File dbPath )
    {
        for ( File file : dbPath.listFiles() )
        {
            if ( !file.getName().equals( INTERNAL_LOG_NAME ) )
            {
                try
                {
                    org.neo4j.io.fs.FileUtils.deleteDirectory( file.toPath() );
                }
                catch ( IOException e )
                {
                    System.err.println( "Couldn't delete " + file + " while cleaning up after successful run, due to " + e );
                }
            }
        }
    }
}
