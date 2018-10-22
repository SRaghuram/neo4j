/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.test.rule.CommercialDatabaseRule;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.integration.bolt.AuthTestBase;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.TestDirectory;

public class SystemGraphAndCredentialsOnlyIT extends AuthTestBase
{
    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();
        EnterpriseAuthAndUserManager authManager = dbRule.resolveDependency( EnterpriseAuthAndUserManager.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        userManager.newUser( NONE_USER, getPassword().getBytes(), false );
        userManager.newUser( READ_USER, getPassword().getBytes(), false );
        userManager.newUser( WRITE_USER, getPassword().getBytes(), false );
        userManager.newUser( PROC_USER, getPassword().getBytes(), false );
        userManager.addRoleToUser( PredefinedRoles.READER, READ_USER );
        userManager.addRoleToUser( PredefinedRoles.PUBLISHER, WRITE_USER );
        userManager.newRole( "procRole", PROC_USER );
    }

    @SuppressWarnings( "deprecation" )
    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        Map<Setting<?>, String> settings = new HashMap<>();
        settings.put( SecuritySettings.auth_providers, SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        settings.put( SecuritySettings.procedure_roles, "test.staticReadProcedure:procRole" );
        return settings;
    }

    @Override
    protected DatabaseRule getDatabaseTestRule( TestDirectory testDirectory )
    {
        return new CommercialDatabaseRule( testDirectory ).startLazily();
    }

    @Override
    protected String getPassword()
    {
        return "abc123";
    }
}
