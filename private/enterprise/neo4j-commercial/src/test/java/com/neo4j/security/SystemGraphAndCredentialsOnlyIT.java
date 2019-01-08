/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.auth.integration.bolt.NativeAndCredentialsOnlyIT;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.rule.CommercialDbmsRule;

import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

public class SystemGraphAndCredentialsOnlyIT extends NativeAndCredentialsOnlyIT
{
    @SuppressWarnings( "deprecation" )
    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        return Collections.singletonMap( SecuritySettings.auth_providers, SecuritySettings.SYSTEM_GRAPH_REALM_NAME + ",plugin-TestCredentialsOnlyPlugin" );
    }

    @Override
    protected DbmsRule getDatabaseTestRule( TestDirectory testDirectory )
    {
        return new CommercialDbmsRule( testDirectory ).startLazily();
    }
}
