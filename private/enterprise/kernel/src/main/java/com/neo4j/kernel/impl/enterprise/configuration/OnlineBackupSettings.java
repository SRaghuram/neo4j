/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.ConfigurationMigrator;
import org.neo4j.kernel.configuration.Migrator;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.prefixSetting;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for online backup
 */
@Description( "Online backup configuration settings" )
public class OnlineBackupSettings implements LoadableConfig
{
    public static final int DEFAULT_BACKUP_PORT = 6362;

    @Description( "Enable support for running online backups." )
    public static final Setting<Boolean> online_backup_enabled = setting( "dbms.backup.enabled", BOOLEAN, TRUE );

    @Description( "Network interface and port for the backup server to listen on." )
    public static final Setting<ListenSocketAddress> online_backup_listen_address = listenAddress( "dbms.backup.listen_address", DEFAULT_BACKUP_PORT );

    @Description( "Name of the SSL policy to be used by backup, as defined under the dbms.ssl.policy.* settings." +
            " If no policy is configured then the communication will not be secured." )
    public static final Setting<String> ssl_policy = prefixSetting( "dbms.backup.ssl_policy", STRING, NO_DEFAULT );

    @SuppressWarnings( "unused" ) // accessed by reflection
    @Migrator
    private static final ConfigurationMigrator migrator = new OnlineBackupConfigurationMigrator();
}
