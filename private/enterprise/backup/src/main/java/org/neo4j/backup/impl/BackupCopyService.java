/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;

import static java.lang.String.format;

class BackupCopyService
{
    private static final int MAX_OLD_BACKUPS = 1000;

    private final FileSystemAbstraction fs;
    private final FileMoveProvider fileMoveProvider;

    BackupCopyService( FileSystemAbstraction fs, FileMoveProvider fileMoveProvider )
    {
        this.fs = fs;
        this.fileMoveProvider = fileMoveProvider;
    }

    void moveBackupLocation( Path oldLocation, Path newLocation ) throws IOException
    {
        try
        {
            File source = oldLocation.toFile();
            File target = newLocation.toFile();
            Iterator<FileMoveAction> moves = fileMoveProvider.traverseForMoving( source ).iterator();
            while ( moves.hasNext() )
            {
                moves.next().move( target );
            }
            oldLocation.toFile().delete();
        }
        catch ( IOException e )
        {
            throw new IOException( "Failed to rename backup directory from " + oldLocation + " to " + newLocation, e );
        }
    }

    void clearIdFiles( Path backupLocation ) throws IOException
    {
        IOException exception = null;
        File targetDirectory = backupLocation.toFile();
        File[] files = fs.listFiles( targetDirectory );
        for ( File file : files )
        {
            if ( !fs.isDirectory( file ) && file.getName().endsWith( ".id" ) )
            {
                try
                {
                    long highId = IdGeneratorImpl.readHighId( fs, file );
                    fs.deleteFile( file );
                    IdGeneratorImpl.createGenerator( fs, file, highId, true );
                }
                catch ( IOException e )
                {
                    exception = Exceptions.chain( exception, e );
                }
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    boolean backupExists( DatabaseLayout databaseLayout )
    {
        return databaseLayout.metadataStore().exists();
    }

    Path findNewBackupLocationForBrokenExisting( Path existingBackup )
    {
        return findAnAvailableBackupLocation( existingBackup, "%s.err.%d" );
    }

    Path findAnAvailableLocationForNewFullBackup( Path desiredBackupLocation )
    {
        return findAnAvailableBackupLocation( desiredBackupLocation, "%s.temp.%d" );
    }

    /**
     * Given a desired file name, find an available name that is similar to the given one that doesn't conflict with already existing backups
     *
     * @param file desired ideal file name
     * @param pattern pattern to follow if desired name is taken (requires %s for original name, and %d for iteration)
     * @return the resolved file name which can be the original desired, or a variation that matches the pattern
     */
    private Path findAnAvailableBackupLocation( Path file, String pattern )
    {
        if ( backupExists( DatabaseLayout.of( file.toFile() ) ) )
        {
            // find alternative name
            final AtomicLong counter = new AtomicLong( 0 );
            Consumer<Path> countNumberOfFilesProcessedForPotentialErrorMessage =
                    generatedBackupFile -> counter.getAndIncrement();

            return availableAlternativeNames( file, pattern )
                    .peek( countNumberOfFilesProcessedForPotentialErrorMessage )
                    .filter( f -> !backupExists( DatabaseLayout.of( f.toFile() ) ) )
                    .findFirst()
                    .orElseThrow( noFreeBackupLocation( file, counter ) );
        }
        return file;
    }

    private static Supplier<RuntimeException> noFreeBackupLocation( Path file, AtomicLong counter )
    {
        return () -> new RuntimeException( String.format(
                "Unable to find a free backup location for the provided %s. %d possible locations were already taken.",
                file, counter.get() ) );
    }

    private static Stream<Path> availableAlternativeNames( Path originalBackupDirectory, String pattern )
    {
        return IntStream.range( 0, MAX_OLD_BACKUPS )
                .mapToObj( iteration -> alteredBackupDirectoryName( pattern, originalBackupDirectory, iteration ) );
    }

    private static Path alteredBackupDirectoryName( String pattern, Path directory, int iteration )
    {
        Path directoryName = directory.getName( directory.getNameCount() - 1 );
        return directory.resolveSibling( format( pattern, directoryName, iteration ) );
    }
}
