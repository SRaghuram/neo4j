/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.backup.impl.BackupExecutionException;
import com.neo4j.backup.impl.ConsistencyCheckExecutionException;
import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.ClusterMember;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.transaction_advertised_address;
import static org.neo4j.internal.helpers.Exceptions.findCauseOrSuppressed;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class BackupHelper
{
    private static final Set<Class<? extends Throwable>> BENIGN_EXCEPTIONS = asSet(
            ConnectException.class,
            ClosedChannelException.class
    );

    private static final Set<Class<? extends Throwable>> STORE_COPY_EXCEPTIONS = asSet(
            StoreCopyFailedException.class,
            StoreIdDownloadFailedException.class
    );

    private static final String DB_NAME = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    AtomicLong backupNumber = new AtomicLong();
    AtomicLong successfulBackups = new AtomicLong();

    private final FileSystemAbstraction fs;
    private final File baseBackupDir;
    private final Log log;

    BackupHelper( Resources resources )
    {
        this.fs = resources.fileSystem();
        this.baseBackupDir = resources.backupDir();
        this.log = resources.logProvider().getLog( getClass() );
    }

    /**
     * Performs a backup and returns the path to it. Benign failures are swallowed and an empty optional gets returned.
     *
     * @param member The member to perform the backup against.
     * @return The optional backup.
     * @throws BackupExecutionException if the backup fails for an unexpected reason during the backup phase.
     * @throws ConsistencyCheckExecutionException if the backup fails during the consistency checking phase.
     */
    Optional<File> backup( ClusterMember member ) throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        SocketAddress address = member.config().get( transaction_advertised_address );
        Path backupDir = createBackupDir( DB_NAME );

        try
        {
            OnlineBackupContext context = OnlineBackupContext.builder()
                    .withDatabaseName( DB_NAME )
                    .withAddress( address.getHostname(), address.getPort() )
                    .withBackupDirectory( backupDir )
                    .withReportsDirectory( backupDir )
                    .build();

            OnlineBackupExecutor.buildDefault().executeBackup( context );
            log.info( String.format( "Created backup %s from %s", backupDir, member ) );

            successfulBackups.incrementAndGet();

            return Optional.of( backupDir.resolve( DB_NAME ).toFile() );
        }
        catch ( BackupExecutionException e )
        {
            // TODO: Fix backup error hierarchy so that regular benign transient errors can be clearly distinguished.

            /* We don't know if these are benign or not, but we use some inside
               knowledge of the implementation to make a reasonable guess. The basic
               idea is to treat IOExceptions as hard failures, regardless of exception
               wrapping, any other store-copy related exception as benign, and everything
               else also as hard and unexpected failures. */

            if ( findCauseOrSuppressed( e, t -> t.getClass().equals( IOException.class ) ).isPresent() )
            {
                throw e;
            }

            Optional<Throwable> benignException = findCauseOrSuppressed( e, t -> STORE_COPY_EXCEPTIONS.contains( t.getClass() ) );
            if ( benignException.isPresent() )
            {
                // StoreCopyException which is not caused by IOException
                log.info( "Benign failure: " + benignException.get().getMessage() );
            }
            else
            {
                throw e;
            }
        }
        catch ( Exception e )
        {
            Optional<Throwable> benignException = findCauseOrSuppressed( e, t -> BENIGN_EXCEPTIONS.contains( t.getClass() ) );
            if ( benignException.isPresent() )
            {
                log.info( "Benign failure: " + benignException.get().getMessage() );
            }
            else
            {
                throw e;
            }
        }
        return Optional.empty();
    }

    /**
     * Construct a path equivalent to `--backup-dir` parameter.
     */
    private Path createBackupDir( String databaseName )
    {
        try
        {
            String backupSubDirName = databaseName + "-backup-" + backupNumber.getAndIncrement();
            File backupDir = new File( baseBackupDir, backupSubDirName );
            fs.mkdirs( backupDir );
            return backupDir.toPath();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
