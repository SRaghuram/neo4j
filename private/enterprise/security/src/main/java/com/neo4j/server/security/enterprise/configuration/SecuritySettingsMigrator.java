/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.configuration;

import java.util.Map;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.configuration.SettingMigrators;
import org.neo4j.logging.Log;

import static com.neo4j.configuration.SecuritySettings.authentication_providers;
import static com.neo4j.configuration.SecuritySettings.authorization_providers;
import static com.neo4j.configuration.SecuritySettings.ldap_authentication_use_attribute;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@ServiceProvider
public class SecuritySettingsMigrator implements SettingMigrator
{
    @Override
    public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
    {
        String value = values.remove( "dbms.security.auth_provider" );
        if ( isNotBlank( value ) )
        {
            log.warn( "Use of deprecated setting dbms.security.auth_provider. It is replaced by " +
                    "dbms.security.authentication_providers and dbms.security.authorization_providers" );
            values.putIfAbsent( authentication_providers.name(), value );
            values.putIfAbsent( authorization_providers.name(), value );
        }

        SettingMigrators.migrateSettingNameChange( values, log, "dbms.security.ldap.authentication.use_samaccountname", ldap_authentication_use_attribute );
    }
}
