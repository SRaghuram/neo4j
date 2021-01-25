/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

@EnterpriseDbmsExtension( configurationCallback = "configuration" )
class EnterpriseRelationshipTypeScanStoreIT
{
    private static final RelationshipType REL_TYPE = RelationshipType.withName( "REL_TYPE" );
    private static final RelationshipType OTHER_REL_TYPE = RelationshipType.withName( "OTHER_REL_TYPE" );
    private static final Label ALLOWED_LABEL = Label.label( "Allowed" );
    private static final Label RESTRICTED_LABEL = Label.label( "Restricted" );
    private static final String USER = "user";
    private static final String ROLE = "role";
    private static final String PASSWORD = "abc123";
    @Inject
    DatabaseManagementService dbms;
    @Inject
    GraphDatabaseService db;
    @Inject
    GraphDatabaseAPI graphDatabaseAPI;
    @Inject
    Kernel kernel;

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
        builder.setConfig( GraphDatabaseSettings.auth_enabled, true );
    }

    @Test
    void shouldCorrectlyCountRelationshipsByType() throws Throwable
    {
        // given a user with restricted access to a label
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = system.beginTx() )
        {
            createRestrictedRole( tx, ROLE );
            createUserWithRole( tx, USER, ROLE );

            tx.commit();
        }

        // and given a set of relevant and irrelevant relationships with restricted and allowed labels attached
        int expectedNbrOfRelationships = 100;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < expectedNbrOfRelationships; i++ )
            {
                // Correct for count
                createRelationship( tx );
                // Wrong type
                createRelationship( tx, OTHER_REL_TYPE );
                // Restricted start label
                createRelationship( tx, RESTRICTED_LABEL, REL_TYPE, ALLOWED_LABEL );
                // Restricted end label
                createRelationship( tx, ALLOWED_LABEL, REL_TYPE, RESTRICTED_LABEL );
            }
            tx.commit();
        }

        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, getLoginContext( USER ) ) )
        {
            Read read = tx.dataRead();
            TokenRead tokenRead = tx.tokenRead();
            int type = tokenRead.relationshipType( REL_TYPE.name() );

            // when
            long count = read.countsForRelationshipWithoutTxState( ANY_LABEL, type, ANY_LABEL );

            // then
            assertThat( count ).isEqualTo( expectedNbrOfRelationships );
            tx.commit();
        }
    }

    private static void createRestrictedRole( Transaction tx, String role )
    {
        tx.execute( "CREATE ROLE " + role );
        tx.execute( "GRANT ACCESS ON DATABASES * TO " + role );
        tx.execute( "GRANT TRAVERSE ON GRAPH * NODES * TO " + role );
        tx.execute( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO " + role );
        tx.execute( "DENY TRAVERSE ON GRAPH * NODES " + RESTRICTED_LABEL + " TO " + role );
    }

    private static void createUserWithRole( Transaction tx, String user, String role )
    {
        tx.execute( "CREATE USER " + user + " SET PASSWORD '" + PASSWORD + "' CHANGE NOT REQUIRED" );
        tx.execute( "GRANT ROLE " + role + " TO " + user );
    }

    private static void createRelationship( Transaction tx )
    {
        createRelationship( tx, REL_TYPE );
    }

    private static void createRelationship( Transaction tx, RelationshipType type )
    {
        createRelationship( tx, ALLOWED_LABEL, type, ALLOWED_LABEL );
    }

    private static void createRelationship( Transaction tx, Label startLabel, RelationshipType type, Label endLabel )
    {
        tx.createNode( startLabel ).createRelationshipTo( tx.createNode( endLabel ), type );
    }

    private LoginContext getLoginContext( String user ) throws InvalidAuthTokenException
    {
        AuthManager authManager = graphDatabaseAPI.getDependencyResolver().resolveDependency( AuthManager.class );
        return authManager.login( Map.of( "principal", user, "credentials", PASSWORD.getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
