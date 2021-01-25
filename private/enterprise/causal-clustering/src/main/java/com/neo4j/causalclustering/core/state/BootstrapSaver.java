/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.catchup.storecopy.StoreFiles.EXCLUDE_TEMPORARY_DIRS;
import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME;

/**
 * Encapsulates a set of behaviours necessary during the binding of the system database in a cluster.
 *
 * The bootstrapping process manipulates the system database seed as part of binding which means that
 * it no longer is consistent with the seed on other instances. That in turn means that those non-bootstrappers
 * need to have their databases replaced. However, that replacement process could fail, so instead of straight
 * up removing the database, we simply store it somewhere else temporarily until we know that the replacement
 * process (i.e. store copy) has completed successfully.
 *
 * In the case that the replacement process fails, for example due to the bootstrapper failing, then the saved
 * database can be moved back prior to a subsequent binding attempt.
 */
public class BootstrapSaver
{
    private final FileSystemAbstraction fileSystem;
    private final Log log;

    public BootstrapSaver( FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Saves the database store files and transactions logs, if they exist, into temporary sub-directories.
     *
     * Intended to be invoked during Raft binding on the non-bootstrappers.
     */
    public void save( DatabaseLayout databaseLayout ) throws IOException
    {
        saveFiles( databaseLayout.databaseDirectory() );
        saveFiles( databaseLayout.getTransactionLogsDirectory() );
    }

    /**
     * Restores previously saved database files, if they exist.
     *
     * Intended to be invoked before binding commences.
     */
    public void restore( DatabaseLayout databaseLayout ) throws IOException
    {
        restoreFiles( databaseLayout.databaseDirectory() );
        restoreFiles( databaseLayout.getTransactionLogsDirectory() );
    }

    /**
     * Cleans any temporarily saved database files.
     *
     * Intended to be invoked after binding and store replacement has finished successfully.
     */
    public void clean( DatabaseLayout databaseLayout ) throws IOException
    {
        clean( databaseLayout.databaseDirectory() );
        clean( databaseLayout.getTransactionLogsDirectory() );
    }

    private void saveFiles( Path directory ) throws IOException
    {
        if ( !hasFiles( directory ) )
        {
            return;
        }

        log.info( "Saving: " + directory );
        saveInSelf( directory );
        assertExistsAndEmpty( directory );
    }

    private boolean hasFiles( Path databaseDirectory )
    {
        Path[] dbFsNodes = fileSystem.listFiles( databaseDirectory, EXCLUDE_TEMPORARY_DIRS );
        return dbFsNodes != null && dbFsNodes.length > 0;
    }

    private void assertExistsAndEmpty( Path databaseDirectory )
    {
        Path[] dbFsNodes;
        dbFsNodes = fileSystem.listFiles( databaseDirectory, EXCLUDE_TEMPORARY_DIRS );
        if ( dbFsNodes == null || dbFsNodes.length != 0 )
        {
            throw new IllegalStateException( "Expected empty directory: " + databaseDirectory );
        }
    }

    private void saveInSelf( Path baseDir ) throws IOException
    {
        Path tempSavedDir = baseDir.resolve( TEMP_SAVE_DIRECTORY_NAME );

        if ( fileSystem.fileExists( tempSavedDir ) )
        {
            throw new IllegalStateException( "Directory not expected to exist: " + tempSavedDir );
        }

        Path tempRenameDir = tempRenameDir( baseDir );

        fileSystem.renameFile( baseDir, tempRenameDir );
        if ( !fileSystem.mkdir( baseDir ) )
        {
            throw new IllegalStateException( "Failed to create: " + baseDir );
        }
        fileSystem.renameFile( tempRenameDir, tempSavedDir );
    }

    private void restoreFiles( Path directory ) throws IOException
    {
        Path tempSaveDir = directory.resolve( TEMP_SAVE_DIRECTORY_NAME );

        if ( !fileSystem.fileExists( tempSaveDir ) )
        {
            return;
        }

        Path[] dbFsNodes = fileSystem.listFiles( directory, EXCLUDE_TEMPORARY_DIRS );

        if ( dbFsNodes == null )
        {
            return;
        }
        else if ( dbFsNodes.length > 0 )
        {
            throw new IllegalStateException( "Unexpected files in directory: " + directory );
        }

        log.info( "Restoring: " + tempSaveDir );
        restoreFromSelf( directory );
    }

    private void restoreFromSelf( Path directory ) throws IOException
    {
        Path tempRenameDir = tempRenameDir( directory );
        Path tempSavedDir = directory.resolve( TEMP_SAVE_DIRECTORY_NAME );
        fileSystem.renameFile( tempSavedDir, tempRenameDir );

        Path[] fsNodes = fileSystem.listFiles( directory, EXCLUDE_TEMPORARY_DIRS );
        if ( fsNodes == null || fsNodes.length != 0 )
        {
            throw new IllegalStateException( "Unexpected state of directory: " + Arrays.toString( fsNodes ) );
        }
        fileSystem.deleteRecursively( directory );
        fileSystem.renameFile( tempRenameDir, directory );
    }

    private Path tempRenameDir( Path baseDir )
    {
        Path tempRenameDir = baseDir.resolveSibling( baseDir.getFileName() + "-" + UUID.randomUUID().toString().substring( 0, 8 ) );
        if ( fileSystem.fileExists( tempRenameDir ) )
        {
            throw new IllegalStateException( "Directory conflict: " + tempRenameDir );
        }
        return tempRenameDir;
    }

    private void clean( Path dbDir ) throws IOException
    {
        Path tempSavedDir = dbDir.resolve( TEMP_SAVE_DIRECTORY_NAME );
        log.info( "Cleaning: " + tempSavedDir );
        fileSystem.deleteRecursively( tempSavedDir );
    }
}
