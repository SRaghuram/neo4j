/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.neo4j.kernel.configuration.Settings.TRUE;

public class DatabaseConfiguration
{
    private DatabaseConfiguration()
    {
        // no instances
    }

    public static Map<String,String> configureTxLogRotationAndPruning( Map<String,String> settings, String txPrune )
    {
        settings.put( GraphDatabaseSettings.keep_logical_logs.name(), txPrune );
        settings.put( GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M" );
        return settings;
    }

    public static Map<String,String> configureBackup( Map<String,String> settings, String hostname, int port )
    {
        settings.put( OnlineBackupSettings.online_backup_enabled.name(), TRUE );
        settings.put( OnlineBackupSettings.online_backup_listen_address.name(), hostname + ":" + port );
        return settings;
    }
}
