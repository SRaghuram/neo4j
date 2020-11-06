/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;
import com.neo4j.configuration.OnlineBackupSettings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

public class OnlineBackupContext
{
    private final SocketAddress address;
    private final DatabaseNamePattern databaseNamePattern;
    private final Path backupDirectory;
    private final Path reportDir;
    private final boolean fallbackToFullBackup;
    private final boolean consistencyCheck;
    private final ConsistencyFlags consistencyFlags;
    private final Config config;
    private final MemoryTracker memoryTracker;
    private final IncludeMetadata includeMetadata;

    private OnlineBackupContext( SocketAddress address, DatabaseNamePattern databaseNamePattern, Path backupDirectory,
                                 Path reportDir, boolean fallbackToFullBackup, boolean consistencyCheck, ConsistencyFlags consistencyFlags, Config config,
                                 MemoryTracker memoryTracker, IncludeMetadata includeMetadata )
    {
        this.address = address;
        this.databaseNamePattern = databaseNamePattern;
        this.backupDirectory = backupDirectory;
        this.reportDir = reportDir;
        this.fallbackToFullBackup = fallbackToFullBackup;
        this.consistencyCheck = consistencyCheck;
        this.consistencyFlags = consistencyFlags;
        this.config = config;
        this.memoryTracker = memoryTracker;
        this.includeMetadata = includeMetadata;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public SocketAddress getAddress()
    {
        return address;
    }

    public DatabaseNamePattern getDatabaseNamePattern()
    {
        return databaseNamePattern;
    }

    public Path getBackupDir()
    {
        return backupDirectory;
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

    public Optional<IncludeMetadata> getIncludeMetadata()
    {
        return Optional.ofNullable( includeMetadata );
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
        private DatabaseNamePattern databaseNamePattern;
        private Path backupDirectory;
        private Path reportsDirectory;
        private IncludeMetadata includeMetadata;
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

        public Builder withDatabaseNamePattern( String databaseName )
        {
            this.databaseNamePattern = new DatabaseNamePattern( databaseName );
            return this;
        }

        public Builder withDatabaseNamePattern( DatabaseNamePattern databaseNamePattern )
        {
            this.databaseNamePattern = databaseNamePattern;
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

        public Builder withIncludeMetadata( IncludeMetadata includeMetadata )
        {
            this.includeMetadata = includeMetadata;
            return this;
        }

        public Config getConfig()
        {
            return config == null ? Config.defaults() : Config.newBuilder().fromConfig( config ).build();
        }

        public DatabaseNamePattern getDatabaseNamePattern()
        {
            return databaseNamePattern != null ? databaseNamePattern : new DatabaseNamePattern( getConfig().get( GraphDatabaseSettings.default_database ) );
        }

        public SocketAddress getAddress()
        {
            return address;
        }

        public OnlineBackupContext build()
        {
            config = getConfig();

            databaseNamePattern = getDatabaseNamePattern();

            if ( backupDirectory == null )
            {
                backupDirectory = Paths.get( "." );
            }
            if ( reportsDirectory == null )
            {
                reportsDirectory = Paths.get( "." );
            }

            SocketAddress socketAddress = buildAddress();
            Path databaseBackupDirectory = backupDirectory;
            ConsistencyFlags consistencyFlags = buildConsistencyFlags();
            var memoryTracker = EmptyMemoryTracker.INSTANCE;

            return new OnlineBackupContext( socketAddress, databaseNamePattern, databaseBackupDirectory, reportsDirectory,
                                            fallbackToFullBackup, consistencyCheck, consistencyFlags, config, memoryTracker, includeMetadata );
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
