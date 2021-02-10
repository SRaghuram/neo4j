/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.lang.String.format;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;

public class FileManager
{
    static final String WORKING_DIR_PATTERN = "%s.tmp.%d";
    static final String ERROR_DIR_PATTERN = "%s.err.%d";
    private final FileSystemAbstraction fs;

    public FileManager( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    boolean directoryDoesNotExistOrIsEmpty( Path directory ) throws IOException
    {
        return !fs.fileExists( directory ) || (fs.isDirectory( directory ) && fs.listFiles( directory ).length == 0);
    }

    void copyDelete( Path directory, Path to ) throws IOException
    {
        if ( !fs.fileExists( directory ) )
        {
            return;
        }
        fs.copyRecursively( directory, to );
        fs.deleteRecursively( directory );
    }

    Path nextWorkingDir( Path dir ) throws IOException
    {
        return createAnAvailableLocation( dir, WORKING_DIR_PATTERN );
    }

    Path nextErrorDir( Path dir ) throws IOException
    {
        return createAnAvailableLocation( dir, ERROR_DIR_PATTERN );
    }

    private Path createAnAvailableLocation( Path path, String pattern ) throws IOException
    {
        Path location;
        int i = 0;
        do
        {
            location = alteredBackupDirectoryName( pattern, path, i++ );
        }
        while ( !isEmptyOrNonExistingDirectory( fs, location ) );
        fs.mkdir( location );
        return location;
    }

    private static Path alteredBackupDirectoryName( String pattern, Path directory, int iteration )
    {
        Path directoryName = directory.getFileName();
        return directory.resolveSibling( format( pattern, directoryName, iteration ) );
    }

    public boolean exists( Path path )
    {
        return fs.fileExists( path );
    }

    public void deleteDir( Path dir ) throws IOException
    {
        fs.deleteRecursively( dir );
    }
}
