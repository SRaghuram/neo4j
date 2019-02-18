/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public abstract class ConfiguredAuthScenariosInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{
    @Override
    public void setUp()
    {
        // tests are required to setup database with specific configs
    }

    @Test
    void shouldAllowRoleCallCreateNewTokensProceduresWhenConfigured() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.default_allowed.name(), "role1" ) );
        userManager.newRole( "role1", "noneSubject" );
        assertEmpty( noneSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertEmpty( noneSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertEmpty( noneSubject, "CALL db.createProperty('MySpecialProperty')" );
    }

    @Test
    void shouldWarnWhenUsingInternalAndOtherProvider() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.auth_providers.name(), internalSecurityName() + " ,LDAP" ) );
        assertSuccess( adminSubject, "CALL dbms.security.listUsers",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( userList ) ) );
        GraphDatabaseFacade localGraph = neo.getLocalGraph();
        InternalTransaction transaction = localGraph
                .beginTransaction( KernelTransaction.Type.explicit, StandardCommercialLoginContext.AUTH_DISABLED );
        Result result =
                localGraph.execute( transaction, "EXPLAIN CALL dbms.security.listUsers", EMPTY_MAP );
        String description = String.format( "%s (%s)", Status.Procedure.ProcedureWarning.code().description(),
                "dbms.security.listUsers only applies to native users." );
        assertThat( containsNotification( result, description ), equalTo( true ) );
        transaction.success();
        transaction.close();
    }

    @Test
    void shouldNotWarnWhenOnlyUsingInternalProvider() throws Throwable
    {
        configuredSetup( stringMap( SecuritySettings.auth_provider.name(), internalSecurityName() ) );
        assertSuccess( adminSubject, "CALL dbms.security.listUsers",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( userList ) ) );
        GraphDatabaseFacade localGraph = neo.getLocalGraph();
        InternalTransaction transaction = localGraph
                .beginTransaction( KernelTransaction.Type.explicit, StandardCommercialLoginContext.AUTH_DISABLED );
        Result result =
                localGraph.execute( transaction, "EXPLAIN CALL dbms.security.listUsers", EMPTY_MAP );
        String description = String.format( "%s (%s)", Status.Procedure.ProcedureWarning.code().description(),
                "dbms.security.listUsers only applies to native users." );
        assertThat( containsNotification( result, description ), equalTo( false ) );
        transaction.success();
        transaction.close();
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return obj;
    }

    private Map<String,Object> userList = map(
            "adminSubject", listOf( ADMIN ),
            "readSubject", listOf( READER ),
            "schemaSubject", listOf( ARCHITECT ),
            "writeSubject", listOf( PUBLISHER ),
            "editorSubject", listOf( EDITOR ),
            "pwdSubject", listOf(),
            "noneSubject", listOf(),
            "neo4j", listOf( ADMIN )
    );

    private boolean containsNotification( Result result, String description )
    {
        Iterator<Notification> itr = result.getNotifications().iterator();
        boolean found = false;
        while ( itr.hasNext() )
        {
            found |= itr.next().getDescription().equals( description );
        }
        return found;
    }

    protected String internalSecurityName()
    {
        return SecuritySettings.NATIVE_REALM_NAME;
    }
}
