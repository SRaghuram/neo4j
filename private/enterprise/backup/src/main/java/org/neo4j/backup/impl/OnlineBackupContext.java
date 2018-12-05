/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

public class OnlineBackupContext
{
    private static final String DEFAULT_BACKUP_NAME = "backup";

    private final OnlineBackupRequiredArguments requiredArguments;
    private final Config config;
    private final ConsistencyFlags consistencyFlags;

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

    public static final class Builder
    {
        private OptionalHostnamePort hostnamePort;
        private String databaseName;
        private Path backupDirectory;
        private String backupName;
        private Path reportsDirectory;
        private boolean fallbackToFullBackup = true;
        private Duration timeout;
        private Config config;
        private boolean consistencyCheck = true;
        private Boolean consistencyCheckGraph;
        private Boolean consistencyCheckIndexes;
        private Boolean consistencyCheckLabelScanStore;
        private Boolean consistencyCheckPropertyOwners;

        private Builder()
        {
        }

        public Builder withHostnamePort( String hostname, int port )
        {
            this.hostnamePort = new OptionalHostnamePort( hostname, port, null );
            return this;
        }

        public Builder withHostnamePort( OptionalHostnamePort hostnamePort )
        {
            this.hostnamePort = hostnamePort;
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

        public Builder withTimeout( Duration timeout )
        {
            this.timeout = timeout;
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

            OptionalHostnamePort address = buildAddress();
            OnlineBackupRequiredArguments requiredArgs = buildRequiredArgs( address );
            ConsistencyFlags consistencyFlags = buildConsistencyFlags();

            return new OnlineBackupContext( requiredArgs, config, consistencyFlags );
        }

        private OptionalHostnamePort buildAddress()
        {
            if ( hostnamePort == null )
            {
                ListenSocketAddress defaultAddress = config.get( OnlineBackupSettings.online_backup_listen_address );
                hostnamePort = new OptionalHostnamePort( defaultAddress.getHostname(), defaultAddress.getPort(), null );
            }
            return hostnamePort;
        }

        private OnlineBackupRequiredArguments buildRequiredArgs( OptionalHostnamePort address )
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
                backupName = DEFAULT_BACKUP_NAME;
            }
            if ( reportsDirectory == null )
            {
                reportsDirectory = Paths.get( "." );
            }
            return new OnlineBackupRequiredArguments( address, databaseName, backupDirectory, backupName,
                    fallbackToFullBackup, consistencyCheck, timeout, reportsDirectory );
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
