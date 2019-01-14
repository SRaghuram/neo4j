/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.AuthProceduresInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SystemGraphEmbeddedUserManagementProceduresInteractionIT extends AuthProceduresInteractionTestBase<EnterpriseLoginContext>
{
    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config )
            throws Throwable
    {
        return new SystemGraphEmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Test
    void shouldUnassignAnyDbRolesWhenDeletingRole() throws Exception
    {
        userManager.newRole( "new_role" );
        userManager.addRoleToUser( "new_role", "noneSubject" );
        assertTrue( ((SystemGraphRealm) userManager).getDbNamesForUser( "noneSubject" )
                        .contains( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ),
                "Should be connected to default db" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteRole('new_role')" );
        assertFalse( ((SystemGraphRealm) userManager).getDbNamesForUser( "noneSubject" )
                        .contains( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ),
                "Should not be connected to default db" );
    }

}
