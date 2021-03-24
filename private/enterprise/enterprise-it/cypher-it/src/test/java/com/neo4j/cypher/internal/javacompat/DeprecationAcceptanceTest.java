/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal.javacompat;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.cypher.internal.javacompat.NotificationTestSupport.containsItem;
import static org.neo4j.cypher.internal.javacompat.NotificationTestSupport.containsNoItem;
import static org.neo4j.cypher.internal.javacompat.NotificationTestSupport.notification;

@ImpermanentEnterpriseDbmsExtension( configurationCallback = "configure" )
public class DeprecationAcceptanceTest
{
    @Inject
    DatabaseManagementService dbms;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.auth_enabled, true );
    }

    // DEPRECATED ENTERPRISE SYNTAX in 4.X

    @Test
    void deprecatedGrantOnDefaultDatabaseSyntax()
    {
        assertNotificationsInSupportedVersions( "explain GRANT ACCESS ON DEFAULT DATABASE TO role", containsItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void deprecatedDenyOnDefaultDatabaseSyntax()
    {
        assertNotificationsInSupportedVersions( "explain DENY START ON DEFAULT DATABASE TO role", containsItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void deprecatedRevokeOnDefaultDatabaseSyntax()
    {
        assertNotificationsInSupportedVersions( "explain REVOKE ALL ON DEFAULT DATABASE FROM role", containsItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void grantOnHomeDatabaseShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain GRANT ACCESS ON HOME DATABASE TO role", containsNoItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void denyOnHomeDatabaseShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain DENY START ON HOME DATABASE TO role", containsNoItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void revokeOnHomeDatabaseShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain REVOKE ALL ON HOME DATABASE FROM role", containsNoItem( deprecatedDefaultDatabaseWarning ) );
    }

    @Test
    void deprecatedGrantOnDefaultGraphSyntax()
    {
        assertNotificationsInSupportedVersions( "explain GRANT TRAVERSE ON DEFAULT GRAPH TO role", containsItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void deprecatedDenyOnDefaultGraphSyntax()
    {
        assertNotificationsInSupportedVersions( "explain DENY WRITE ON DEFAULT GRAPH TO role", containsItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void deprecatedRevokeOnDefaultGraphSyntax()
    {
        assertNotificationsInSupportedVersions( "explain REVOKE ALL ON DEFAULT GRAPH FROM role", containsItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void grantOnHomeGraphShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain GRANT TRAVERSE ON HOME GRAPH TO role", containsNoItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void denyOnHomeGraphShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain DENY WRITE ON HOME GRAPH TO role", containsNoItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void revokeOnHomeGraphShouldNotBeDepreacted()
    {
        assertNotificationsInSupportedVersions( "explain REVOKE ALL ON HOME GRAPH FROM role", containsNoItem( deprecatedDefaultGraphWarning ) );
    }

    @Test
    void deprecatedCatalogKeywordForAdminCommands()
    {
        // technically deprecated in both community and enterprise, but needs to be run against system database
        assertNotificationsInSupportedVersions( "EXPLAIN CATALOG SHOW USERS", containsItem( deprecatedCatalogSyntax ));
        assertNotificationsInSupportedVersions( "EXPLAIN CATALOG CREATE ROLE foo", containsItem( deprecatedCatalogSyntax ));
        assertNotificationsInSupportedVersions( "EXPLAIN CATALOG DROP DATABASE foo IF EXISTS", containsItem( deprecatedCatalogSyntax ));
        assertNotificationsInSupportedVersions( "EXPLAIN CATALOG SHOW PRIVILEGES AS COMMANDS", containsItem( deprecatedCatalogSyntax ));
    }

    // MATCHERS & HELPERS

    void assertNotificationsInSupportedVersions( String query, Matcher<Iterable<Notification>> matchesExpectation )
    {
        assertNotifications( List.of( "CYPHER 4.3" ), query, matchesExpectation );
    }

    private void assertNotifications( List<String> versions, String query, Matcher<Iterable<Notification>> matchesExpectation )
    {
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        versions.forEach( version ->
        {
            try ( Transaction transaction = system.beginTx() )
            {
                try ( Result result = transaction.execute( String.format("%s %s", version, query) ) )
                {
                    assertThat( result.getNotifications(), matchesExpectation );
                }
            }
        } );
    }

    private final Matcher<Notification> deprecatedDefaultDatabaseWarning =
            deprecation( "The `ON DEFAULT DATABASE` syntax is deprecated, use `ON HOME DATABASE` instead" );

    private final Matcher<Notification> deprecatedDefaultGraphWarning =
            deprecation( "The `ON DEFAULT GRAPH` syntax is deprecated, use `ON HOME GRAPH` instead" );

    private final Matcher<Notification> deprecatedCatalogSyntax =
            deprecation( "The optional `CATALOG` prefix for administration commands has been deprecated and should be omitted." );

    private static Matcher<Notification> deprecation( String message )
    {
        return notification( "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                             containsString( message ), any( InputPosition.class ), SeverityLevel.WARNING );
    }
}
