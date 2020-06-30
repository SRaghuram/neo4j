/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.configuration.SecuritySettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public abstract class ConfiguredAuthScenariosInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{
    @Override
    public void setUp( TestInfo testInfo )
    {
        // tests are required to setup database with specific configs
    }

    @Test
    void shouldAllowRoleCallCreateNewTokensProceduresWhenConfigured( TestInfo testInfo ) throws Throwable
    {
        configuredSetup( Map.of( GraphDatabaseSettings.default_allowed, "role1" ), testInfo );
        createRoleWithAccess( "role1", "noneSubject" );
        assertEmpty( noneSubject, "CALL db.createLabel('MySpecialLabel')" );
        assertEmpty( noneSubject, "CALL db.createRelationshipType('MySpecialRelationship')" );
        assertEmpty( noneSubject, "CALL db.createProperty('MySpecialProperty')" );
    }

    @Test
    void shouldWarnWhenUsingInternalAndOtherProvider( TestInfo testInfo ) throws Throwable
    {
        configuredSetup( Map.of(
                SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME,
                SecuritySettings.authorization_providers, SecuritySettings.NATIVE_REALM_NAME + "," + SecuritySettings.LDAP_REALM_NAME ),
                testInfo );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsers",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( userList ) ) );
        GraphDatabaseFacade localGraph = neo.getLocalGraph();
        try ( Transaction transaction1 = localGraph.beginTx() )
        {
            Result result = transaction1.execute( "EXPLAIN CALL dbms.security.listUsers" );
            String description =
                    String.format( "%s (%s)", Status.Procedure.ProcedureWarning.code().description(), "dbms.security.listUsers only applies to native users." );
            assertThat( containsNotification( result, description ) ).isEqualTo( true );
        }
    }

    @Test
    void shouldNotWarnWhenOnlyUsingInternalProvider( TestInfo testInfo ) throws Throwable
    {
        configuredSetup( Map.of(
                SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers, SecuritySettings.NATIVE_REALM_NAME
        ), testInfo );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.listUsers",
                r -> assertKeyIsMap( r, "username", "roles", valueOf( userList ) ) );
        GraphDatabaseFacade localGraph = neo.getLocalGraph();
        try ( Transaction transaction1 = localGraph.beginTx() )
        {
            Result result = transaction1.execute( "EXPLAIN CALL dbms.security.listUsers" );
            String description =
                    String.format( "%s (%s)", Status.Procedure.ProcedureWarning.code().description(), "dbms.security.listUsers only applies to native users." );
            assertThat( containsNotification( result, description ) ).isEqualTo( false );
        }
    }

    @Override
    protected Object valueOf( Object obj )
    {
        return obj;
    }

    private final Map<String,Object> userList = map(
            "adminSubject", listOf( PUBLIC,  ADMIN ),
            "readSubject", listOf( PUBLIC,  READER ),
            "schemaSubject", listOf( PUBLIC,  ARCHITECT ),
            "writeSubject", listOf( PUBLIC,  PUBLISHER ),
            "editorSubject", listOf( PUBLIC,  EDITOR ),
            "pwdSubject", listOf( PUBLIC ),
            "noneSubject", listOf( PUBLIC ),
            "neo4j", listOf( PUBLIC,  ADMIN )
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
}
