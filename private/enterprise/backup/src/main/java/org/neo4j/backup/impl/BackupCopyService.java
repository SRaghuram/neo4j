/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;
import static org.neo4j.io.fs.FileSystemUtils.isEmptyOrNonExistingDirectory;

class BackupCopyService
{
    private static final int MAX_OLD_BACKUPS = 1000;

    private final FileSystemAbstraction fs;
    private final FileMoveProvider fileMoveProvider;
    private final PageCache pageCache;
    private final Log log;

    BackupCopyService( FileSystemAbstraction fs, FileMoveProvider fileMoveProvider, PageCache pageCache, LogProvider logProvider )
    {
        this.fs = fs;
        this.fileMoveProvider = fileMoveProvider;
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
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
            fs.deleteRecursively( oldLocation.toFile() );
        }
        catch ( IOException e )
        {
            throw new IOException( "Failed to rename backup directory from " + oldLocation + " to " + newLocation, e );
        }
    }

    void clearIdFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        IOException exception = null;
        for ( File file : databaseLayout.idFiles() )
        {
            try
            {
                if ( fs.fileExists( file ) )
                {
                    long highId = IdGeneratorImpl.readHighId( fs, file );
                    fs.deleteFile( file );
                    IdGeneratorImpl.createGenerator( fs, file, highId, true );
                }
            }
            catch ( IOException e )
            {
                exception = Exceptions.chain( exception, e );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    void deletePreExistingBrokenBackupIfPossible( Path preExistingBrokenBackupDir, Path newSuccessfulBackupDir ) throws IOException
    {
        DatabaseLayout preExistingBrokenBackupLayout = DatabaseLayout.of( preExistingBrokenBackupDir.toFile() );
        DatabaseLayout newSuccessfulBackupLayout = DatabaseLayout.of( newSuccessfulBackupDir.toFile() );

        StoreId preExistingBrokenBackupStoreId;
        try
        {
            preExistingBrokenBackupStoreId = MetaDataStore.getStoreId( pageCache, preExistingBrokenBackupLayout.metadataStore() );
        }
        catch ( IOException e )
        {
            log.warn( "Unable to read store ID from the pre-existing invalid backup. It will not be deleted", e );
            return;
        }

        StoreId newSuccessfulBackupStoreId = MetaDataStore.getStoreId( pageCache, newSuccessfulBackupLayout.metadataStore() );

        if ( newSuccessfulBackupStoreId.equals( preExistingBrokenBackupStoreId ) )
        {
            log.info( "Deleting the pre-existing invalid backup because its store ID is the same as in the new successful backup %s",
                    newSuccessfulBackupStoreId );

            fs.deleteRecursively( preExistingBrokenBackupDir.toFile() );
        }
        else
        {
            log.info( "Pre-existing invalid backup can't be deleted because its store ID %s is not the same as in the new successful backup %s",
                    preExistingBrokenBackupStoreId, newSuccessfulBackupStoreId );
        }
    }

    boolean backupExists( DatabaseLayout databaseLayout )
    {
        return fs.fileExists( databaseLayout.metadataStore() );
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
        if ( isEmptyOrNonExistingDirectory( fs, file.toFile() ) )
        {
            return file;
        }

        return availableAlternativeNames( file, pattern )
                .filter( f -> isEmptyOrNonExistingDirectory( fs, f.toFile() ) )
                .findFirst()
                .orElseThrow( noFreeBackupLocation( file ) );
    }

    private static Supplier<RuntimeException> noFreeBackupLocation( Path file )
    {
        return () -> new RuntimeException( format(
                "Unable to find a free backup location for the provided %s. %d possible locations were already taken.",
                file, MAX_OLD_BACKUPS ) );
    }

    private static Stream<Path> availableAlternativeNames( Path originalBackupDirectory, String pattern )
    {
        return IntStream.range( 0, MAX_OLD_BACKUPS )
                .mapToObj( iteration -> alteredBackupDirectoryName( pattern, originalBackupDirectory, iteration ) );
    }

    private static Path alteredBackupDirectoryName( String pattern, Path directory, int iteration )
    {
        Path directoryName = directory.getFileName();
        return directory.resolveSibling( format( pattern, directoryName, iteration ) );
    }
}
