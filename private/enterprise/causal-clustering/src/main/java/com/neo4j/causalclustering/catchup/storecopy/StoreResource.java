/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class StoreResource
{
    private final Path path;
    private final String relativePath;
    private final int recordSize;
    private final FileSystemAbstraction fs;

    public StoreResource( Path path, String relativePath, int recordSize, FileSystemAbstraction fs )
    {
        this.path = path;
        this.relativePath = relativePath;
        this.recordSize = recordSize;
        this.fs = fs;
    }

    StoreChannel open() throws IOException
    {
        return fs.read( path );
    }

    public String relativePath()
    {
        return relativePath;
    }

    Path path()
    {
        return path;
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
        return recordSize == that.recordSize && Objects.equals( path, that.path ) && Objects.equals( relativePath, that.relativePath );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( path, relativePath, recordSize );
    }

    @Override
    public String toString()
    {
        return "StoreResource{" + "path='" + relativePath + '\'' + ", recordSize=" + recordSize + '}';
    }
}
