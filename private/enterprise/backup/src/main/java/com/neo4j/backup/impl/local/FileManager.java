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
    private static final int DEFAULT_MAX_TEST_DIRS = 1000;
    static final String WORKING_DIR_PATTERN = "%s.tmp.%d";
    static final String ERROR_DIR_PATTERN = "%s.err.%d";
    private final FileSystemAbstraction fs;
    private final int maxDirAttempts;

    public FileManager( FileSystemAbstraction fs, int maxDirAttempts )
    {
        this.fs = fs;
        this.maxDirAttempts = maxDirAttempts;
    }

    FileManager( FileSystemAbstraction fs )
    {
        this( fs, DEFAULT_MAX_TEST_DIRS );
    }

    boolean directoryDoesNotExistOrIsEmpty( Path directory )
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

    Path nextWorkingDir( Path dir )
    {
        return createAnAvailableLocation( dir, WORKING_DIR_PATTERN );
    }

    Path nextErrorDir( Path dir )
    {
        return createAnAvailableLocation( dir, ERROR_DIR_PATTERN );
    }

    private Path createAnAvailableLocation( Path path, String pattern )
    {
        var location = availableAlternativeNames( path, pattern, maxDirAttempts )
                .filter( f -> isEmptyOrNonExistingDirectory( fs, f ) )
                .findFirst()
                .orElseThrow( noFreeBackupLocation( path, maxDirAttempts ) );
        fs.mkdir( location );
        return location;
    }

    private static Supplier<RuntimeException> noFreeBackupLocation( Path file, int maxDirAttempts )
    {
        return () -> new RuntimeException( format(
                "Unable to find a free backup location for the provided %s. %d possible locations were already taken.",
                file, maxDirAttempts ) );
    }

    private static Stream<Path> availableAlternativeNames( Path originalBackupDirectory, String pattern, int maxDirAttempts )
    {
        return IntStream.range( 0, maxDirAttempts )
                        .mapToObj( iteration -> alteredBackupDirectoryName( pattern, originalBackupDirectory, iteration ) );
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
