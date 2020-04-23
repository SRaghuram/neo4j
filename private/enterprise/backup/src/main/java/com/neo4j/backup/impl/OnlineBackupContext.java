/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

public class OnlineBackupContext
{
    private final SocketAddress address;
    private final String databaseName;
    private final Path databaseBackupDir;
    private final Path reportDir;
    private final boolean fallbackToFullBackup;
    private final boolean consistencyCheck;
    private final ConsistencyFlags consistencyFlags;
    private final Config config;
    private final MemoryTracker memoryTracker;

    private OnlineBackupContext( SocketAddress address, String databaseName, Path databaseBackupDir, Path reportDir, boolean fallbackToFullBackup,
            boolean consistencyCheck, ConsistencyFlags consistencyFlags, Config config, MemoryTracker memoryTracker )
    {
        this.address = address;
        this.databaseName = databaseName;
        this.databaseBackupDir = databaseBackupDir;
        this.reportDir = reportDir;
        this.fallbackToFullBackup = fallbackToFullBackup;
        this.consistencyCheck = consistencyCheck;
        this.consistencyFlags = consistencyFlags;
        this.config = config;
        this.memoryTracker = memoryTracker;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public SocketAddress getAddress()
    {
        return address;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Path getDatabaseBackupDir()
    {
        return databaseBackupDir;
    }

    public boolean fallbackToFullBackupEnabled()
    {
        return fallbackToFullBackup;
    }

    public boolean consistencyCheckEnabled()
    {
        return consistencyCheck;
    }

    public Path getReportDir()
    {
        return reportDir;
    }

    Config getConfig()
    {
        return config;
    }

    ConsistencyFlags getConsistencyFlags()
    {
        return consistencyFlags;
    }

    public MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }

    public static final class Builder
    {
        private SocketAddress address;
        private String databaseName;
        private Path backupDirectory;
        private Path reportsDirectory;
        private boolean fallbackToFullBackup = true;
        private Config config;
        private boolean consistencyCheck = true;
        private boolean consistencyCheckGraph = true;
        private boolean consistencyCheckIndexes = true;
        private boolean consistencyCheckIndexStructure = true;
        private boolean consistencyCheckLabelScanStore = true;
        private boolean consistencyCheckRelationshipTypeScanStore = true;
        private boolean consistencyCheckPropertyOwners;

        private Builder()
        {
        }

        public Builder withAddress( String hostname, int port )
        {
            return withAddress( new SocketAddress( hostname, port ) );
        }

        public Builder withAddress( SocketAddress address )
        {
            this.address = address;
            return this;
        }

        public Builder withDatabaseName( String databaseName )
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withBackupDirectory( Path backupDirectory )
        {
            this.backupDirectory = backupDirectory;
            return this;
        }

        public Builder withReportsDirectory( Path reportsDirectory )
        {
            this.reportsDirectory = reportsDirectory;
            return this;
        }

        public Builder withFallbackToFullBackup( boolean fallbackToFullBackup )
        {
            this.fallbackToFullBackup = fallbackToFullBackup;
            return this;
        }

        public Builder withConfig( Config config )
        {
            this.config = config;
            return this;
        }

        public Builder withConsistencyCheck( boolean consistencyCheck )
        {
            this.consistencyCheck = consistencyCheck;
            return this;
        }

        public Builder withConsistencyCheckGraph( Boolean consistencyCheckGraph )
        {
            this.consistencyCheckGraph = consistencyCheckGraph;
            return this;
        }

        public Builder withConsistencyCheckIndexes( Boolean consistencyCheckIndexes )
        {
            this.consistencyCheckIndexes = consistencyCheckIndexes;
            return this;
        }

        public Builder withConsistencyCheckIndexStructure( Boolean consistencyCheckIndexStructure )
        {
            this.consistencyCheckIndexStructure = consistencyCheckIndexStructure;
            return this;
        }

        public Builder withConsistencyCheckLabelScanStore( Boolean consistencyCheckLabelScanStore )
        {
            this.consistencyCheckLabelScanStore = consistencyCheckLabelScanStore;
            return this;
        }

        public Builder withConsistencyCheckRelationshipTypeScanStore( Boolean consistencyCheckRelationshipTypeScanStore )
        {
            this.consistencyCheckRelationshipTypeScanStore = consistencyCheckRelationshipTypeScanStore;
            return this;
        }

        public Builder withConsistencyCheckPropertyOwners( Boolean consistencyCheckPropertyOwners )
        {
            this.consistencyCheckPropertyOwners = consistencyCheckPropertyOwners;
            return this;
        }

        public OnlineBackupContext build()
        {
            if ( config == null )
            {
                config = Config.defaults();
            }
            if ( databaseName == null )
            {
                databaseName = config.get( GraphDatabaseSettings.default_database );
            }
            if ( backupDirectory == null )
            {
                backupDirectory = Paths.get( "." );
            }
            if ( reportsDirectory == null )
            {
                reportsDirectory = Paths.get( "." );
            }

            SocketAddress socketAddress = buildAddress();
            Path databaseBackupDirectory = backupDirectory.resolve( databaseName );
            ConsistencyFlags consistencyFlags = buildConsistencyFlags();
            var memoryTracker = EmptyMemoryTracker.INSTANCE;

            return new OnlineBackupContext( socketAddress, databaseName, databaseBackupDirectory, reportsDirectory,
                    fallbackToFullBackup, consistencyCheck, consistencyFlags, config, memoryTracker );
        }

        private SocketAddress buildAddress()
        {
            if ( address == null )
            {
                SocketAddress defaultListenAddress = config.get( OnlineBackupSettings.online_backup_listen_address );
                address = new SocketAddress( defaultListenAddress.getHostname(), defaultListenAddress.getPort() );
            }
            return address;
        }

        private ConsistencyFlags buildConsistencyFlags()
        {
            return new ConsistencyFlags( consistencyCheckGraph, consistencyCheckIndexes, consistencyCheckIndexStructure, consistencyCheckLabelScanStore,
                    consistencyCheckRelationshipTypeScanStore, consistencyCheckPropertyOwners );
        }
    }
}
