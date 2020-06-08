/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class StoreResource
{
    private final File file;
    private final String path;
    private final int recordSize;
    private final FileSystemAbstraction fs;

    public StoreResource( File file, String relativePath, int recordSize, FileSystemAbstraction fs )
    {
        this.file = file;
        this.path = relativePath;
        this.recordSize = recordSize;
        this.fs = fs;
    }

    StoreChannel open() throws IOException
    {
        return fs.read( file );
    }

    public String path()
    {
        return path;
    }

    File file()
    {
        return file;
    }

    int recordSize()
    {
        return recordSize;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        StoreResource that = (StoreResource) o;
        return recordSize == that.recordSize && Objects.equals( file, that.file ) && Objects.equals( path, that.path );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( file, path, recordSize );
    }

    @Override
    public String toString()
    {
        return "StoreResource{" + "path='" + path + '\'' + ", recordSize=" + recordSize + '}';
    }
}
