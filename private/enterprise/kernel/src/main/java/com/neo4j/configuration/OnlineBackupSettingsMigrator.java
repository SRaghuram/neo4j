/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.Map;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.SettingMigrators;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;

@ServiceProvider
public class OnlineBackupSettingsMigrator implements SettingMigrator
{
    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        SettingMigrators.migrateSettingNameChange( values, log, "dbms.backup.address", online_backup_listen_address );
    }
}
