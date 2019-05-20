/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.ListenSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class OnlineBackupContext
{
    private final AdvertisedSocketAddress address;
    private final DatabaseId databaseId;
    private final Path databaseBackupDir;
    private final Path reportDir;
    private final boolean fallbackToFullBackup;
    private final boolean consistencyCheck;
    private final ConsistencyFlags consistencyFlags;
    private final Config config;

    private OnlineBackupContext( AdvertisedSocketAddress address, DatabaseId databaseId, Path databaseBackupDir, Path reportDir, boolean fallbackToFullBackup,
            boolean consistencyCheck, ConsistencyFlags consistencyFlags, Config config )
    {
        this.address = address;
        this.databaseId = databaseId;
        this.databaseBackupDir = databaseBackupDir;
        this.reportDir = reportDir;
        this.fallbackToFullBackup = fallbackToFullBackup;
        this.consistencyCheck = consistencyCheck;
        this.consistencyFlags = consistencyFlags;
        this.config = config;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public AdvertisedSocketAddress getAddress()
    {
        return address;
    }

    public DatabaseId getDatabaseId()
    {
        return databaseId;
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

    public static final class Builder
    {
        private AdvertisedSocketAddress address;
        private DatabaseId databaseId;
        private Path backupDirectory;
        private Path reportsDirectory;
        private boolean fallbackToFullBackup = true;
        private Config config;
        private boolean consistencyCheck = true;
        private Boolean consistencyCheckGraph;
        private Boolean consistencyCheckIndexes;
        private Boolean consistencyCheckLabelScanStore;
        private Boolean consistencyCheckPropertyOwners;

        private Builder()
        {
        }

        public Builder withAddress( String hostname, int port )
        {
            return withAddress( new AdvertisedSocketAddress( hostname, port ) );
        }

        public Builder withAddress( AdvertisedSocketAddress address )
        {
            this.address = address;
            return this;
        }

        public Builder withDatabaseId( DatabaseId databaseId )
        {
            this.databaseId = databaseId;
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

        public Builder withConsistencyCheckLabelScanStore( Boolean consistencyCheckLabelScanStore )
        {
            this.consistencyCheckLabelScanStore = consistencyCheckLabelScanStore;
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
            if ( databaseId == null )
            {
                databaseId = new DatabaseId( config.get( GraphDatabaseSettings.default_database ) );
            }
            if ( backupDirectory == null )
            {
                backupDirectory = Paths.get( "." );
            }
            if ( reportsDirectory == null )
            {
                reportsDirectory = Paths.get( "." );
            }

            AdvertisedSocketAddress address = buildAddress();
            Path databaseBackupDirectory = backupDirectory.resolve( databaseId.name() );
            ConsistencyFlags consistencyFlags = buildConsistencyFlags();

            return new OnlineBackupContext( address, databaseId, databaseBackupDirectory, reportsDirectory,
                    fallbackToFullBackup, consistencyCheck, consistencyFlags, config );
        }

        private AdvertisedSocketAddress buildAddress()
        {
            if ( address == null )
            {
                ListenSocketAddress defaultListenAddress = config.get( OnlineBackupSettings.online_backup_listen_address );
                address = new AdvertisedSocketAddress( defaultListenAddress.getHostname(), defaultListenAddress.getPort() );
            }
            return address;
        }

        private ConsistencyFlags buildConsistencyFlags()
        {
            if ( consistencyCheckGraph == null )
            {
                consistencyCheckGraph = config.get( ConsistencyCheckSettings.consistency_check_graph );
            }
            if ( consistencyCheckIndexes == null )
            {
                consistencyCheckIndexes = config.get( ConsistencyCheckSettings.consistency_check_indexes );
            }
            if ( consistencyCheckLabelScanStore == null )
            {
                consistencyCheckLabelScanStore = config.get( ConsistencyCheckSettings.consistency_check_label_scan_store );
            }
            if ( consistencyCheckPropertyOwners == null )
            {
                consistencyCheckPropertyOwners = config.get( ConsistencyCheckSettings.consistency_check_property_owners );
            }
            return new ConsistencyFlags( consistencyCheckGraph, consistencyCheckIndexes, consistencyCheckLabelScanStore,
                    consistencyCheckPropertyOwners );
        }
    }
}
