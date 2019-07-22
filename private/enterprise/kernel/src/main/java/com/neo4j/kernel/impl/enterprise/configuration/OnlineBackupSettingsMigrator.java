/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import java.util.Map;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.Log;

@ServiceProvider
public class OnlineBackupSettingsMigrator implements SettingMigrator
{
    private static final String OLD_SETTING = "dbms.backup.address";
    private static final String NEW_SETTING = OnlineBackupSettings.online_backup_listen_address.name();

    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        String value = values.remove( OLD_SETTING );
        if ( value != null )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", OLD_SETTING, NEW_SETTING );
            values.putIfAbsent( NEW_SETTING, value );
        }
    }
}
