/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.junit.Test;

import java.util.Map;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.server.security.enterprise.auth.AuthProceduresInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void shouldUnassignAnyDbRolesWhenDeletingRole() throws Exception
    {
        userManager.newRole( "new_role" );
        userManager.addRoleToUser( "new_role", "noneSubject" );
        assertTrue( "Should be connected to default db",
                ((SystemGraphRealm) userManager).getDbNamesForUser( "noneSubject" ).contains( DatabaseManager.DEFAULT_DATABASE_NAME ) );
        assertEmpty( adminSubject, "CALL dbms.security.deleteRole('new_role')" );
        assertFalse( "Should not be connected to default db",
                ((SystemGraphRealm) userManager).getDbNamesForUser( "noneSubject" ).contains( DatabaseManager.DEFAULT_DATABASE_NAME ) );
    }

}
