/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import org.neo4j.kernel.configuration.BaseConfigurationMigrator;

class OnlineBackupConfigurationMigrator extends BaseConfigurationMigrator
{
    OnlineBackupConfigurationMigrator()
    {
        add( new BackupAddressMigrator() );
    }

    private static class BackupAddressMigrator extends SpecificPropertyMigration
    {
        static final String OLD_SETTING = "dbms.backup.address";
        static final String NEW_SETTING = OnlineBackupSettings.online_backup_listen_address.name();

        BackupAddressMigrator()
        {
            super( OLD_SETTING, OLD_SETTING + " has been replaced with " + NEW_SETTING + "." );
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            if ( StringUtils.isNotEmpty( value ) )
            {
                rawConfiguration.put( NEW_SETTING, value );
            }
        }
    }
}
