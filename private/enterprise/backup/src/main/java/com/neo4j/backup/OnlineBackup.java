/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.BackupExecutionException;
import com.neo4j.backup.impl.ConsistencyCheckExecutionException;
import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.configuration.OnlineBackupSettings;

import java.io.OutputStream;
import java.nio.file.Path;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * This class encapsulates the information needed to perform an online backup against a running Neo4j instance
 * configured to act as a backup server. This class is not instantiable, instead factory methods are used to get
 * instances configured to contact a specific backup server against which all possible backup operations can be
 * performed.
 * <p>
 * All backup methods return the same instance, allowing for chaining calls.
 *
 * @see OnlineBackup#from(String)
 * @see OnlineBackup#from(String, int)
 */
@PublicApi
public final class OnlineBackup
{
    private final String hostnameOrIp;
    private final int port;
    private final Config config;

    private boolean fallbackToFullBackup = true;
    private boolean consistencyCheck = true;
    private OutputStream outputStream = System.out;

    private OnlineBackup( String hostnameOrIp, int port )
    {
        this.hostnameOrIp = hostnameOrIp;
        this.port = port;
        this.config = Config.defaults();
    }

    /**
     * Factory method for this class. The returned instance will perform backup operations against the
     * hostname passed in as parameter, using the default backup port {@value OnlineBackupSettings#DEFAULT_BACKUP_PORT}.
     *
     * @param hostnameOrIp hostname or IP address of the backup server.
     * @return an {@code OnlineBackup} instance ready to perform backup operations from the given remote server.
     * @throws NullPointerException if the parameter is {@code null}.
     */
    public static OnlineBackup from( String hostnameOrIp )
    {
        return from( hostnameOrIp, OnlineBackupSettings.DEFAULT_BACKUP_PORT );
    }

    /**
     * Factory method for this class. The returned instance will perform backup operations against the
     * hostname and port passed in as parameters.
     *
     * @param hostnameOrIp hostname or IP address of the backup server.
     * @param port port at which the remote backup server is listening. Value should positive and less than or equal to 65535.
     * @return an {@code OnlineBackup} instance ready to perform backup operations from the given remote server.
     * @throws NullPointerException if the hostname parameter is {@code null}.
     * @throws IllegalArgumentException if the port parameter is not in the expected range.
     */
    public static OnlineBackup from( String hostnameOrIp, int port )
    {
        return new OnlineBackup( requireNonNull( hostnameOrIp, "hostnameOrIp" ), requireValidPort( port ) );
    }

    /**
     * Allow fallback to a full backup if an incremental backup fails.
     * This configuration value is only applied when the target directory already contains a backup.
     * <p>
     * Default value is {@code true}.
     *
     * @param fallbackToFullBackup {@code true} if fallback to a full backup is allowed, {@code false} otherwise.
     * @return this {@code OnlineBackup} instance.
     */
    public OnlineBackup withFallbackToFullBackup( boolean fallbackToFullBackup )
    {
        this.fallbackToFullBackup = fallbackToFullBackup;
        return this;
    }

    /**
     * Allow consistency check after a successful backup.
     * <p>
     * Status of the consistency check will be available via {@link Result#isConsistent()}.
     * Consistency check report will be written to the target directory.
     * <p>
     * Default value is {@code true}.
     *
     * @param consistencyCheck {@code true} if a successful backup should be checked for consistency, {@code false} otherwise.
     * @return this {@code OnlineBackup} instance.
     */
    public OnlineBackup withConsistencyCheck( boolean consistencyCheck )
    {
        this.consistencyCheck = consistencyCheck;
        return this;
    }

    /**
     * Specify an {@link OutputStream output stream} for the backup to use for printing out status messages and progress information.
     * <p>
     * Default value is {@link System#out}.
     *
     * @param outputStream the output stream to use.
     * @return this {@code OnlineBackup} instance.
     * @throws NullPointerException if the parameter is {@code null};
     */
    public OnlineBackup withOutputStream( OutputStream outputStream )
    {
        this.outputStream = requireNonNull( outputStream, "outputStream" );
        return this;
    }

    /**
     * Performs a backup of the specified database into the into target directory. The server contacted is the one
     * configured in the factory method used to obtain this instance.
     * <p>
     * After the backup is complete, a consistency check phase will take place, checking the database for consistency.
     * If any errors are found, {@link Result#isConsistent()} will return {@code false} and the consistency check report will
     * be written to the target directory. Consistency check can be turned on/off using {@link #withConsistencyCheck(boolean)}.
     * <p>
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     * <p>
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param databaseName the name of the database to backup.
     * @param targetDirectory directory where the backup should be placed. Can contain a previous full backup.
     * @return the same {@code OnlineBackup} instance, possible to use for a new backup operation.
     */
    public Result backup( String databaseName, Path targetDirectory )
    {
        requireNonNull( databaseName, "databaseName" );
        requireNonNull( targetDirectory, "targetDirectory" );

        LogProvider logProvider = FormattedLogProvider.toOutputStream( outputStream );
        try
        {
            executeBackup( databaseName, targetDirectory, logProvider );
            return new Result( consistencyCheck );
        }
        catch ( BackupExecutionException e )
        {
            throw new RuntimeException( "Backup failed", e );
        }
        catch ( ConsistencyCheckExecutionException e )
        {
            logProvider.getLog( getClass() ).error( "Consistency check failed", e );
            return new Result( false );
        }
    }

    private void executeBackup( String databaseName, Path targetDirectory, LogProvider logProvider )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        OnlineBackupExecutor backupExecutor = OnlineBackupExecutor.builder()
                .withUserLogProvider( logProvider )
                .withProgressMonitorFactory( ProgressMonitorFactory.textual( outputStream ) )
                .build();

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( hostnameOrIp, port )
                .withConfig( config )
                .withConsistencyCheck( consistencyCheck )
                .withFallbackToFullBackup( fallbackToFullBackup )
                .withBackupDirectory( targetDirectory )
                .withReportsDirectory( targetDirectory )
                .withDatabaseName( databaseName )
                .withConsistencyCheckRelationshipTypeScanStore( config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store ) )
                .build();

        backupExecutor.executeBackup( context );
    }

    private static int requireValidPort( int port )
    {
        checkArgument( port > 0 && port <= 65535, "Port is expected to be positive and less than or equal to 65535 but was: " + port );
        return port;
    }

    /**
     * Represents an outcome of a successful backup.
     */
    public static final class Result
    {
        private final boolean consistent;

        private Result( boolean consistent )
        {
            this.consistent = consistent;
        }

        /**
         * Tests whether the executed backup passed a consistency check.
         * <p>
         * Consistency check can be switched on/off using {@link OnlineBackup#withConsistencyCheck(boolean)}.
         *
         * @return {@code true} if a consistency check was performed and was successful, {@code false} otherwise.
         */
        public boolean isConsistent()
        {
            return consistent;
        }
    }
}
