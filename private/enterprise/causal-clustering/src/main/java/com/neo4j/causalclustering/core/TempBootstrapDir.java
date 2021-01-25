/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME;

public class TempBootstrapDir
{
    protected final FileSystemAbstraction fs;
    protected final Path tempBootstrapDir;

    public TempBootstrapDir( FileSystemAbstraction fs, DatabaseLayout layout )
    {
        this.fs = fs;
        this.tempBootstrapDir = layout.databaseDirectory().resolve( TEMP_BOOTSTRAP_DIRECTORY_NAME );
    }

    public Path get()
    {
        return tempBootstrapDir;
    }

    public void delete() throws IOException
    {
        fs.deleteRecursively( tempBootstrapDir );
    }

    public static Resource cleanBeforeAndAfter( FileSystemAbstraction fs, DatabaseLayout layout ) throws IOException
    {
        return new Resource( fs, layout );
    }

    public static class Resource extends TempBootstrapDir implements AutoCloseable
    {
        private Resource( FileSystemAbstraction fs, DatabaseLayout layout ) throws IOException
        {
            super( fs, layout );
            fs.deleteRecursively( get() );
        }

        @Override
        public void close() throws IOException
        {
            fs.deleteRecursively( get() );
        }
    }
}
