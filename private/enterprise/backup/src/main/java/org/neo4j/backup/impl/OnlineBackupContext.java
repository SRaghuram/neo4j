/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.common.Service;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class OnlineBackupContext
{
    private final OnlineBackupRequiredArguments requiredArguments;
    private final Config config;
    private final ConsistencyFlags consistencyFlags;
    private final StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine( Service.loadAll( StorageEngineFactory.class ) );

    OnlineBackupContext( OnlineBackupRequiredArguments requiredArguments, Config config, ConsistencyFlags consistencyFlags )
    {
        this.requiredArguments = requiredArguments;
        this.config = config;
        this.consistencyFlags = consistencyFlags;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    OnlineBackupRequiredArguments getRequiredArguments()
    {
        return requiredArguments;
    }

    Config getConfig()
    {
        return config;
    }

    ConsistencyFlags getConsistencyFlags()
    {
        return consistencyFlags;
    }

    Path getResolvedLocationFromName()
    {
        return requiredArguments.getResolvedLocationFromName();
    }

    StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }

    public static final class Builder
    {
        private AdvertisedSocketAddress address;
        private String databaseName;
        private Path backupDirectory;
        private String backupName;
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

        public Builder withBackupName( String backupName )
        {
            this.backupName = backupName;
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

            AdvertisedSocketAddress address = buildAddress();
            OnlineBackupRequiredArguments requiredArgs = buildRequiredArgs( address );
            ConsistencyFlags consistencyFlags = buildConsistencyFlags();

            return new OnlineBackupContext( requiredArgs, config, consistencyFlags );
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

        private OnlineBackupRequiredArguments buildRequiredArgs( AdvertisedSocketAddress address )
        {
            if ( databaseName == null )
            {
                databaseName = config.get( GraphDatabaseSettings.active_database );
            }
            if ( backupDirectory == null )
            {
                backupDirectory = Paths.get( "." );
            }
            if ( backupName == null )
            {
                backupName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
            }
            if ( reportsDirectory == null )
            {
                reportsDirectory = Paths.get( "." );
            }
            return new OnlineBackupRequiredArguments( address, databaseName, backupDirectory, backupName,
                    fallbackToFullBackup, consistencyCheck, reportsDirectory );
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
