/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.ConfigOptions;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.HostnamePort;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.HOSTNAME_PORT;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Settings for online backup
 */
@Description( "Online backup configuration settings" )
@Deprecated
public class OnlineBackupSettings implements LoadableConfig
{
    @Description( "Enable support for running online backups" )
    @Deprecated
    public static final Setting<Boolean> online_backup_enabled = setting( "dbms.backup.enabled", BOOLEAN, TRUE );

    @Description( "Listening server for online backups" )
    @Deprecated
    public static final Setting<HostnamePort> online_backup_server = setting( "dbms.backup.address", HOSTNAME_PORT,
            "127.0.0.1:6362-6372" );

    @Override
    public List<ConfigOptions> getConfigOptions()
    {
        return new ArrayList<>();
    }
}
