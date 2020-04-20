/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME;

public class TempBootstrapDir
{
    protected final FileSystemAbstraction fs;
    protected final File tempBootstrapDir;

    public TempBootstrapDir( FileSystemAbstraction fs, DatabaseLayout layout )
    {
        this.fs = fs;
        this.tempBootstrapDir = new File( layout.databaseDirectory(), TEMP_BOOTSTRAP_DIRECTORY_NAME );
    }

    public File get()
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
