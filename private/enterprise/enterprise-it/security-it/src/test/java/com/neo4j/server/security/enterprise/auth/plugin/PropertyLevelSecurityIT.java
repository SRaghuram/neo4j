/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.KernelTransaction.Type.explicit;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;

@TestDirectoryExtension
class PropertyLevelSecurityIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseFacade db;
    private GraphDatabaseFacade systemDb;
    private LoginContext neo;
    private LoginContext smith;
    private LoginContext morpheus;
    private LoginContext jones;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws Throwable
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() ).impermanent()
                .setConfig( SecuritySettings.property_level_authorization_enabled, true )
                .setConfig( SecuritySettings.property_level_authorization_permissions, "Agent=alias,secret" )
                .setConfig( GraphDatabaseSettings.procedure_roles, "test.*:procRole" ).setConfig( GraphDatabaseSettings.auth_enabled, true ).build();
        db = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
        systemDb = (GraphDatabaseFacade) managementService.database( SYSTEM_DATABASE_NAME );
        EnterpriseAuthAndUserManager authManager = (EnterpriseAuthAndUserManager) db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        GlobalProcedures globalProcedures = db.getDependencyResolver().resolveDependency( GlobalProcedures.class );
        globalProcedures.registerProcedure( TestProcedure.class );

        newUser( "Neo", "eon" );
        newUser( "Smith","mr" );
        newUser( "Jones", "mr" );
        newUser( "Morpheus", "dealwithit" );

        newRole( "procRole" );
        newRole( "Agent" );

        grantRoleToUser( "procRole", "Jones" );
        grantRoleToUser( "Agent", "Smith" );
        grantRoleToUser( "Agent", "Jones" );
        grantRoleToUser( PredefinedRoles.ARCHITECT, "Neo" );
        grantRoleToUser( PredefinedRoles.EDITOR, "Smith" );
        grantRoleToUser( PredefinedRoles.READER, "Morpheus" );

        neo = authManager.login( authToken( "Neo", "eon" ) );
        smith = authManager.login( authToken( "Smith", "mr" ) );
        jones = authManager.login( authToken( "Jones", "mr" ) );
        morpheus = authManager.login( authToken( "Morpheus", "dealwithit" ) );
        executeOnSystem( "GRANT ACCESS ON DATABASE * TO Agent, procRole" );
    }

    private void newUser( String username, String password )
    {
        executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", username, password ) );
    }

    private void newRole( String name )
    {
        executeOnSystem( String.format( "CREATE ROLE %s", name ) );
    }

    private void grantRoleToUser( String role, String user )
    {
        executeOnSystem( String.format( "GRANT ROLE %s TO %s", role, user ) );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void shouldNotShowRestrictedTokensForRestrictedUser()
    {
        Result result = execute( neo, "CREATE (n {name: 'Andersson', alias: 'neo'}) ", Collections.emptyMap() );
        assertThat( result.getQueryStatistics().getNodesCreated(), equalTo( 1 ) );
        assertThat( result.getQueryStatistics().getPropertiesSet(), equalTo( 2 ) );
        result.close();
        execute( smith, "MATCH (n) WHERE n.name = 'Andersson' RETURN n, n.alias as alias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );
    }

    @Test
    void shouldShowRestrictedTokensForUnrestrictedUser()
    {
        Result result = execute( neo, "CREATE (n {name: 'Andersson', alias: 'neo'}) ", Collections.emptyMap() );
        assertThat( result.getQueryStatistics().getNodesCreated(), equalTo( 1 ) );
        assertThat( result.getQueryStatistics().getPropertiesSet(), equalTo( 2 ) );
        result.close();
        execute( morpheus, "MATCH (n) WHERE n.name = 'Andersson' RETURN n, n.alias as alias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( "neo" ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissing()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) WHERE n.name = 'Andersson' RETURN n.alias as alias";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "alias" ), equalTo( null ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissingWhenFiltering()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) WHERE n.alias = 'neo' RETURN n";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( true ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForKeys()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) RETURN keys(n) AS keys";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "keys" ), equalTo( Collections.singletonList( "name" ) ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "keys" ), equalTo( Collections.singletonList( "name" ) ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( (Iterable<String>) r.next().get( "keys" ), contains( "name", "alias" ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForProperties()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n) RETURN properties(n) AS props";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "name", "Andersson" ) ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "name", "Andersson" ) ) );
        } );

        Map<String, String> expected = new HashMap<>(  );
        expected.put( "name", "Andersson" );
        expected.put( "alias", "neo" );
        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( expected ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForExists()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE exists(n.alias) RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForStringBegins()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.alias starts with 'n' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForNotContains()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE NOT n.alias contains 'eo' RETURN n.alias, n.name";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Betasson', alias: 'beta'}) ", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Cetasson'}) ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            Map<String,Object> next = r.next();
            assertThat( next.get( "n.alias" ), equalTo( "beta" ) );
            assertThat( next.get( "n.name" ), equalTo( "Betasson" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForRange()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.secret > 10 RETURN n.secret";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.secret = 42 ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.secret" ), equalTo( 42L ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForCompositeQuery()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) WHERE n.name = 'Andersson' and n.alias = 'neo' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    // INDEX

    @Test
    void shouldReadBackFromTransactionStateWithIndex()
    {
        execute( neo, "CREATE (:A {secret: 1})", Collections.emptyMap() ).close();

        execute( neo, "CREATE INDEX FOR (n:A) ON (n.secret)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();

        String query = "CREATE (:A {secret: $param}) WITH 1 as bar MATCH (a:A) USING INDEX a:A(secret) WHERE EXISTS(a.secret) RETURN a.secret";
        execute( neo, query, Collections.singletonMap("param", 2L), r -> {
            List<Object> results = r.stream().map( s -> s.get( "a.secret" ) ).collect( toList() );
            assertThat( results.size(), equalTo( 2 ) );
            assertThat( results, containsInAnyOrder( 1L, 2L ) );
        } );
        execute( smith, query, Collections.singletonMap("param", 3L), r -> {
            List<Object> results = r.stream().map( s -> s.get( "a.secret" ) ).collect( toList() );
            assertThat( results.size(), equalTo( 1 ) );
            assertThat( results, containsInAnyOrder( 3L ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissingWhenFilteringWithIndex()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'})", Collections.emptyMap() ).close();
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE n.alias = 'neo' RETURN n";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( true ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForExistsWithIndex()
    {
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE exists(n.alias) RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexScan" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForStringBeginsWithIndex()
    {
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.alias)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(alias) WHERE n.alias starts with 'n' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeekByRange" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForRangeWithIndex()
    {
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.secret)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(secret) WHERE n.secret > 10 RETURN n.secret";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.secret = 42 ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.secret" ), equalTo( 42L ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForCompositeWithIndex()
    {
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.name , n.alias)", Collections.emptyMap() ).close();
        execute( neo, "CREATE INDEX FOR (n:Person) ON (n.name)", Collections.emptyMap() ).close();
        execute( neo, "CALL db.awaitIndexes", Collections.emptyMap() ).close();
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "MATCH (n:Person) USING INDEX n:Person(name, alias) WHERE n.name = 'Andersson' and n.alias = 'neo' RETURN n.alias";

        execute( neo, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.getExecutionPlanDescription().toString(), containsString( "NodeIndexSeek" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "n.alias" ), equalTo( "neo" ) );
        } );

        execute( smith, query, Collections.emptyMap(), r -> assertThat( r.hasNext(), equalTo( false ) ) );
    }

    // RELATIONSHIPS

    @Test
    void shouldBehaveLikeDataIsMissingForRelationshipProperties()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) CREATE (m { name: 'Betasson'}) CREATE (n)-[:Neighbour]->(m)", Collections.emptyMap() ).close();

        String query = "MATCH (n)-[r]->(m) WHERE n.name = 'Andersson' AND m.name = 'Betasson' RETURN properties(r) AS props";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'})-[r]->({name: 'Betasson'}) SET r.secret = 'lovers' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "secret", "lovers" ) ) );
        } );
    }

    @Test
    void shouldBehaveLikeDataIsMissingForRelationshipPropertiesPart2()
    {
        execute( neo, "CREATE (n {name: 'Andersson'}) CREATE (m { name: 'Betasson'}) CREATE (n)-[:Neighbour]->(m)", Collections.emptyMap() ).close();

        //In this case we only read the relationship properties
        String query = "MATCH (n)-[r]->(m) RETURN properties(r) AS props";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'})-[r]->({name: 'Betasson'}) SET r.secret = 'lovers' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.emptyMap() ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "props" ), equalTo( Collections.singletonMap( "secret", "lovers" ) ) );
        } );
    }

    // PROCS

    @Test
    void shouldBehaveWithProcedures()
    {
        execute( neo, "CREATE (n:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        String query = "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey";

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, "MATCH (n {name: 'Andersson'}) SET n.alias = 'neo' ", Collections.emptyMap() ).close();

        execute( smith, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        execute( neo, query, Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "alias" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "name" ) );
            assertThat( r.hasNext(), equalTo( true ) );
            assertThat( r.next().get( "propertyKey" ), equalTo( "secret" ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );
    }

    @Test
    void allowedProcedureShouldIgnorePropertyBlacklist()
    {
        execute( neo, "CREATE (:Person {name: 'Andersson'}) ", Collections.emptyMap() ).close();

        assertProcedureResult( morpheus, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( smith, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( jones, Collections.singletonMap( "Andersson", "N/A" ) );

        execute( neo, "MATCH (n:Person) WHERE n.name = 'Andersson' SET n.alias = 'neo' RETURN n", Collections.emptyMap() ).close();

        assertProcedureResult( morpheus, Collections.singletonMap( "Andersson", "neo" ) );
        assertProcedureResult( smith, Collections.singletonMap( "Andersson", "N/A" ) );
        assertProcedureResult( jones, Collections.singletonMap( "Andersson", "neo" ) );
    }

    private void assertProcedureResult( LoginContext user, Map<String,String> nameAliasMap )
    {
        execute( user, "CALL test.getAlias", Collections.emptyMap(), r ->
        {
            assertThat( r.hasNext(), equalTo( true ) );
            Map<String,Object> next = r.next();
            String name = (String) next.get( "name" );
            assertThat( nameAliasMap.containsKey( name ), equalTo( true ) );
            assertThat( next.get( "alias" ), equalTo( nameAliasMap.get( name ) ) );
        } );
    }

    private void execute( LoginContext subject, String query, Map<String,Object> params, Consumer<Result> consumer )
    {
        Result result;
        try ( InternalTransaction tx = db.beginTransaction( explicit, subject ) )
        {
            result = tx.execute( query, params );
            consumer.accept( result );
            tx.commit();
            result.close();
        }
    }

    private void executeOnSystem( String query )
    {
        try ( Transaction tx = systemDb.beginTx() )
        {
            tx.execute( query);
            tx.commit();
        }
    }

    private Result execute( LoginContext subject, String query, Map<String,Object> params )
    {
        Result result;
        try ( InternalTransaction tx = db.beginTransaction( explicit, subject ) )
        {
            result = tx.execute( query, params );
            result.resultAsString();
            tx.commit();
        }
        return result;
    }

    @SuppressWarnings( "unused" )
    public static class TestProcedure
    {
        @Context
        public Transaction transaction;

        @Procedure( name = "test.getAlias", mode = Mode.READ )
        public Stream<MyOutputRecord> getAlias()
        {
            ResourceIterator<Node> nodes = transaction.findNodes( Label.label( "Person" ) );
            return nodes
                    .stream()
                    .map( n -> new MyOutputRecord( (String) n.getProperty( "name" ),
                                                   (String) n.getProperty( "alias", "N/A" ) ) );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class MyOutputRecord
    {
        public String name;
        public String alias;

        MyOutputRecord( String name, String alias )
        {
            this.name = name;
            this.alias = alias;
        }
    }
}
