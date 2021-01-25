/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import com.neo4j.bench.common.util.BenchmarkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.neo4j.bench.common.util.BenchmarkUtil.deleteDir;

public abstract class Store implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger( Store.class );

    /**
     * Store directory will be automatically deleted on close.
     *
     * @return new temporary Store
     */
    public abstract Store makeTemporaryCopy();

    public abstract Store makeCopyAt( Path topLevelDirCopy );

    public abstract void assertDirectoryIsNeoStore();

    public abstract Path topLevelDirectory();

    public abstract Path graphDbDirectory();

    public abstract DatabaseName databaseName();

    public void removeIndexDir()
    {
        Path indexDir = graphDbDirectory().resolve( "schema/" );
        if ( Files.exists( indexDir ) )
        {
            deleteDir( indexDir );
        }
    }

    public abstract void removeTxLogs();

    abstract boolean isTemporaryCopy();

    public long bytes()
    {
        return BenchmarkUtil.bytes( topLevelDirectory() );
    }

    @Override
    public void close()
    {
        if ( isTemporaryCopy() )
        {
            LOG.debug( "Deleting store: " + topLevelDirectory().toAbsolutePath() );

            BenchmarkUtil.assertDirectoryExists( topLevelDirectory() );
            deleteDir( topLevelDirectory() );
        }
    }
}
