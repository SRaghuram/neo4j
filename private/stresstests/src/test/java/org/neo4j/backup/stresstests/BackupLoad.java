/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.backup.impl.BackupExecutionException;
import org.neo4j.backup.impl.ConsistencyCheckExecutionException;
import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.causalclustering.stresstests.Control;
import org.neo4j.function.Predicates;
import org.neo4j.helper.IsChannelClosedException;
import org.neo4j.helper.IsConnectionException;
import org.neo4j.helper.IsConnectionResetByPeer;
import org.neo4j.helper.IsStoreClosed;
import org.neo4j.helper.Workload;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.FormattedLogProvider;

class BackupLoad extends Workload
{
    private static final Predicate<Throwable> isTransientError = Predicates.any(
            new IsConnectionException(),
            new IsConnectionResetByPeer(),
            new IsChannelClosedException(),
            new IsStoreClosed() );

    private final String backupHostname;
    private final int backupPort;
    private final Path backupDir;

    BackupLoad( Control control, String backupHostname, int backupPort, Path backupDir )
    {
        super( control );
        this.backupHostname = backupHostname;
        this.backupPort = backupPort;
        this.backupDir = backupDir;
    }

    @Override
    protected void doWork() throws Exception
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try
        {
            executeBackup( backupHostname, backupPort, backupDir, outputStream );
        }
        catch ( Exception e )
        {
            if ( isTransientError.test( e ) )
            {
                TimeUnit.MILLISECONDS.sleep( 100 );
            }
            else
            {
                flushToStandardOutput( outputStream );
                throw e;
            }
        }
        finally
        {
            outputStream.close();
        }
    }

    private static void executeBackup( String host, int port, Path targetDir, OutputStream outputStream )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        OnlineBackupContext context = OnlineBackupContext.builder()
                .withHostnamePort( host, port )
                .withBackupDirectory( targetDir )
                .withReportsDirectory( targetDir )
                .withFallbackToFullBackup( true )
                .withConsistencyCheck( true )
                .withConsistencyCheckPropertyOwners( true )
                .build();

        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                .withOutputStream( outputStream )
                .withProgressMonitorFactory( ProgressMonitorFactory.textual( outputStream ) )
                .withLogProvider( FormattedLogProvider.toOutputStream( outputStream ) )
                .build();

        executor.executeBackup( context );
    }

    private static void flushToStandardOutput( ByteArrayOutputStream outputStream )
    {
        try
        {
            outputStream.writeTo( System.out );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
