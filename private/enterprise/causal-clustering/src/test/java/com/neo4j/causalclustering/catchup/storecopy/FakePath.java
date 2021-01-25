/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.nio.file.Path;
import java.util.Optional;

/**
 * A fake file tracks a file, but also several counters and helpers that can be used in tests to invoke desired behaviour
 */
class FakePath
{
    private Path path;
    private String filename;
    private String content;
    private int remainingNoResponse;
    private int remainingFailed;
    private Path relativePath;

    FakePath( String name, String content )
    {
        setFilename( name );
        this.content = content;
    }

    public void setFilename( String filename )
    {
        this.filename = filename;
        this.path = getRelativePath().resolve( filename );
    }

    public void setPath( Path path )
    {
        this.filename = path.getFileName().toString();
        this.path = path;
    }

    private Path getRelativePath()
    {
        return Optional.ofNullable( relativePath ).orElse( Path.of( "." ) );
    }

    public Path getPath()
    {
        return path;
    }

    public String getFilename()
    {
        return filename;
    }

    public String getContent()
    {
        return content;
    }

    public void setContent( String content )
    {
        this.content = content;
    }

    /**
     * Clear response that the file has failed to copy (safe connection close, communication, ...)
     *
     * @return
     */
    int getRemainingFailed()
    {
        return remainingFailed;
    }

    void setRemainingFailed( int remainingFailed )
    {
        this.remainingFailed = remainingFailed;
    }
}
