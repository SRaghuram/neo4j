/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

public class FileHeader
{
    private final String fileName;
    private final int requiredAlignment;

    public FileHeader( String fileName )
    {
        // A required alignment of 1 basically means that any alignment will do.
        this( fileName, 1 );
    }

    public FileHeader( String fileName, int requiredAlignment )
    {
        this.fileName = fileName;
        this.requiredAlignment = requiredAlignment;
    }

    public String fileName()
    {
        return fileName;
    }

    public int requiredAlignment()
    {
        return requiredAlignment;
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
        FileHeader that = (FileHeader) o;
        return requiredAlignment == that.requiredAlignment && Objects.equals( fileName, that.fileName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fileName, requiredAlignment );
    }

    @Override
    public String toString()
    {
        return "FileHeader{" + "fileName='" + fileName + '\'' + ", requiredAlignment=" + requiredAlignment + '}';
    }
}
