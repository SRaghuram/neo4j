/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.auth.ProcedureInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.connectDriver;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class NativeAuthIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    private DbmsRule dbRule = new EnterpriseDbmsRule( testDirectory ).startLazily();

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    private static final String READ_USER = "neo";
    private static final String WRITE_USER = "tank";
    private static final String ADMIN_USER = "neo4j";

    private final String password = "secret";

    private GraphDatabaseFacade systemDb;

    private String boltUri;

    private String getPassword()
    {
        return password;
    }

    @Before
    public void setup() throws Exception
    {
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, true )
                .withSetting( BoltConnector.enabled, true )
                .withSetting( BoltConnector.encryption_level, DISABLED )
                .withSetting( BoltConnector.listen_address, new SocketAddress(  InetAddress.getLoopbackAddress().getHostAddress(), 0 ) )
                .withSetting( SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) )
                .withSetting( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) );
        dbRule.ensureStarted();
        dbRule.resolveDependency( GlobalProcedures.class ).registerProcedure( ProcedureInteractionTestBase.ClassWithProcedures.class );

        systemDb = (GraphDatabaseFacade) dbRule.getManagementService().database( SYSTEM_DATABASE_NAME );
        executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", READ_USER, password ) );
        executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", WRITE_USER, password ) );
        executeOnSystem( String.format( "ALTER USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", ADMIN_USER, password ) );
        executeOnSystem( String.format( "GRANT ROLE %s TO %s", PredefinedRoles.READER, READ_USER ) );
        executeOnSystem( String.format( "GRANT ROLE %s TO %s", PredefinedRoles.PUBLISHER, WRITE_USER ) );
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );
    }

    private void executeOnSystem( String query )
    {
        try ( Transaction tx = systemDb.beginTx() )
        {
            tx.execute( query);
            tx.commit();
        }
    }

    @Test
    public void shouldNotFailOnUpdatingSecurityCommandsOnSystemDb()
    {
        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE DATABASE foo" ).consume();
                session.run( "CREATE ROLE fooRole" ).consume();
                session.run( "CALL dbms.security.createUser('fooUser', 'fooPassword')" ).consume();
                session.run( "GRANT ACCESS ON DATABASE * TO fooRole" ).consume();
                session.run( "GRANT MATCH {foo} ON GRAPH * NODES * TO fooRole" ).consume();
                session.run( "GRANT TRAVERSE ON GRAPH foo TO fooRole" ).consume();
            }
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW ROLE fooRole PRIVILEGES" ).list();
                assertThat( "Should have the right number of underlying privileges", records.size(), equalTo( 4 ) );
                List<Record> grants = records.stream().filter( r -> r.asMap().get( "resource" ).equals( "property(foo)" ) ).collect( Collectors.toList() );
                assertThat( "Should have read access to nodes on all databases", grants.size(), equalTo( 1 ) );
            }
        }
    }

    @Test
    public void shouldNotFailOnDatabaseSchemaProceduresWithReadOnlyUser()
    {
        try ( Driver driver = connectDriver( boltUri, WRITE_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( DEFAULT_DATABASE_NAME ) ) )
            {
                session.run( "CREATE (:A)-[:X]->(:B)<-[:Y]-(:C)" ).consume();
                session.run( "MATCH (n:A) SET n.prop1 = 1" ).consume();
                session.run( "MATCH (n:B) SET n.prop2 = 2" ).consume();
                session.run( "MATCH (n:C) SET n.prop3 = 3" ).consume();
            }
        }

        try ( Driver driver = connectDriver( boltUri, READ_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( DEFAULT_DATABASE_NAME ) ) )
            {
                assertThat( session.run( "CALL db.labels" ).list( this::labelOf ), containsInAnyOrder( List.of( "A", "B", "C" ).toArray() ) );
                assertThat( session.run( "CALL db.relationshipTypes" ).list( this::typeOf ), containsInAnyOrder( List.of( "X", "Y" ).toArray() ) );
                assertThat( session.run( "CALL db.propertyKeys" ).list( this::propOf ), containsInAnyOrder( List.of( "prop1", "prop2", "prop3" ).toArray() ) );
            }
        }
    }

    @Test
    public void shouldNotFailOnDatabaseSchemaProceduresWithRestrictedUserOnNewDatabase()
    {
        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE DATABASE foo" ).consume();
                session.run( "CREATE ROLE custom" ).consume();
                session.run( "CREATE USER joe SET PASSWORD $password CHANGE NOT REQUIRED", map( "password", getPassword() ) ).consume();
                session.run( "GRANT ROLE custom TO joe", map( "password", getPassword() ) ).consume();
                session.run( "GRANT ACCESS ON DATABASE * TO custom" ).consume();
                session.run( "GRANT MATCH {prop1} ON GRAPH * NODES * TO custom" ).consume();
                session.run( "GRANT TRAVERSE ON GRAPH foo TO custom" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH foo NODES A TO custom" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH foo RELATIONSHIPS X TO custom" ).consume();
            }
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW ROLE custom PRIVILEGES" ).list();
                assertThat( "Should have the right number of underlying privileges", records.size(), equalTo( 6 ) );
                List<Record> grants = records.stream().filter( r -> r.asMap().get( "resource" ).equals( "property(prop1)" ) ).collect( Collectors.toList() );
                assertThat( "Should have read access to nodes on all databases", grants.size(), equalTo( 1 ) );

                List<Record> filteredRecords = session.run( "SHOW ROLE custom PRIVILEGES YIELD resource" ).list();
                assertThat( "Should have the right number of underlying privileges", filteredRecords.size(), equalTo( 6 ) );
                List<Record> filteredGrants =
                        filteredRecords.stream().filter( r -> r.asMap().get( "resource" ).equals( "property(prop1)" ) ).collect( Collectors.toList() );
                assertThat( "Should have read access to nodes on all databases", filteredGrants.size(), equalTo( 1 ) );
            }
        }

        try ( Driver driver = connectDriver( boltUri, WRITE_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( "foo" ) ) )
            {
                session.run( "CREATE (:A)-[:X]->(:B)<-[:Y]-(:C)" ).consume();
                session.run( "MATCH (n:A) SET n.prop1 = 1" ).consume();
                session.run( "MATCH (n:B) SET n.prop2 = 2" ).consume();
                session.run( "MATCH (n:C) SET n.prop3 = 3" ).consume();
            }
        }

        Map<String,Map<String,List<String>>> userExpectedResults =
                Map.of( READ_USER, Map.of( "labels", List.of( "A", "B", "C" ), "types", List.of( "X", "Y" ), "props", List.of( "prop1", "prop2", "prop3" ) ),
                        "joe", Map.of( "labels", List.of( "B", "C" ), "types", List.of( "Y" ), "props", List.of( "prop1" ) ) );
        for ( String user : userExpectedResults.keySet() )
        {
            Map<String,List<String>> expectedResults = userExpectedResults.get( user );
            try ( Driver driver = connectDriver( boltUri, user, getPassword() ) )
            {
                try ( Session session = driver.session( forDatabase( "foo" ) ) )
                {
                    assertPropertyFunctionWorks( session, user, "labels", "CALL db.labels", this::labelOf, expectedResults );
                    assertPropertyFunctionWorks( session, user, "types", "CALL db.relationshipTypes", this::typeOf, expectedResults );
                    assertPropertyFunctionWorks( session, user, "props", "CALL db.propertyKeys", this::propOf, expectedResults );
                }
            }
        }
    }

    @Test
    public void shouldNotFailOnDatabaseSchemaProceduresWithRestrictedUserOnNewMoreComplexDatabase()
    {
        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE USER tim SET PASSWORD $password CHANGE NOT REQUIRED", map( "password", getPassword() ) ).consume();
                session.run( "CREATE ROLE role" ).consume();
                session.run( "GRANT ROLE role TO tim" ).consume();
                session.run( "GRANT ACCESS ON DATABASE * TO role" ).consume();
                session.run( "GRANT MATCH {*} ON GRAPH * ELEMENTS * TO role" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH * NODES City TO role" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH * RELATIONSHIPS FROM TO role" ).consume();
                session.run( "DENY READ {name} ON GRAPH * NODES Person TO role" ).consume();
                session.run( "CREATE DATABASE foo" ).consume();
            }
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW ROLE role PRIVILEGES" ).list();
                assertThat( "Should have the right number of underlying privileges", records.size(), equalTo( 6 ) );
                List<Record> grants = records.stream().filter( r -> {
                    Map<String, Object> m = r.asMap();
                    return m.get( "access" ).equals( "DENIED" ) &&
                            m.get( "resource" ).equals( "property(name)" ) &&
                            m.get( "segment" ).equals( "NODE(Person)" );
                } ).collect( Collectors.toList() );
                assertThat( "Should deny read access to ':Person(name)' property", grants.size(), equalTo( 1 ) );
            }
        }

        List<String> labels = List.of( "City", "Donor", "Jur", "Nat", "Party", "Officer", "Company", "PostCode", "State", "Person", "FormerEastGermany",
                "FormerWestGermany" );
        List<String> types =
                List.of( "LOCATED_IN", "BELONGS_TO", "BELONGED_TO", "GOT_REGISTERED_IN", "REGISTERED", "HAS_SEAT_IN", "GENERAL_MANAGER", "IS_MAYBE", "FROM",
                        "PROCURATOR", "LIQUIDATOR", "PERSONALLY_LIABLE_PARTNER", "CHAIRMAN", "OWNER", "DONATED_TO", "IS_LIKELY_TO_BE", "IS_VERY_LIKELY_TO_BE",
                        "IS_MAYBE_BECAUSE_OF_SAME_PLACE" );

        try ( var driver = connectDriver( boltUri, WRITE_USER, getPassword() ) )
        {
            try ( var session = driver.session( forDatabase( "foo" ) ) )
            {
                try ( var tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (:PostCode {name:'postcode'})-[:LOCATED_IN]->(:City {name:'city'})-[:BELONGS_TO]->(:State {name:'state'})" ).consume();
                    for ( String label : labels )
                    {
                        if ( !label.equals( "City" ) )
                        {
                            for ( String relType : types )
                            {
                                if ( !relType.equals( "LOCATED_IN" ) && !relType.equals( "BELONGS_TO" ) )
                                {
                                    tx.run( "CREATE (:" + label + " {name:'" + label + "'})-[:" + relType + "]->(:" + label + ")" ).consume();
                                }
                            }
                        }
                    }
                    tx.commit();
                }
            }
        }

        List<String> timLabels = labels.stream().filter( v -> !v.equals( "City" ) ).collect( Collectors.toList() );
        List<String> timTypes =
                types.stream().filter( v -> !( v.equals( "FROM" )) ).collect( Collectors.toList() );
        Map<String,Map<String,List<String>>> userExpectedResults =
                Map.of( READ_USER, Map.of( "labels", labels, "types", types, "props", List.of( "name" ) ),
                        "tim", Map.of( "labels", timLabels, "types", timTypes, "props", List.of( "name" ) ) );
        for ( String user : userExpectedResults.keySet() )
        {
            Map<String,List<String>> expectedResults = userExpectedResults.get( user );
            try ( Driver driver = connectDriver( boltUri, user, getPassword() ) )
            {
                try ( Session session = driver.session( forDatabase( "foo" ) ) )
                {
                    assertPropertyFunctionWorks( session, user, "labels", "CALL db.labels", this::labelOf, expectedResults );
                    assertPropertyFunctionWorks( session, user, "types", "CALL db.relationshipTypes", this::typeOf, expectedResults );
                    assertPropertyFunctionWorks( session, user, "props", "CALL db.propertyKeys", this::propOf, expectedResults );
                }
            }
        }
    }

    @Test
    public void shouldCreateUserWithAuthDisabled() throws Exception
    {
        // GIVEN
        dbRule.restartDatabase( Collections.singletonMap( GraphDatabaseSettings.auth_enabled, false ) );
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );
        try ( Driver driver = connectDriver( boltUri, AuthTokens.none() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                // WHEN
                session.run( "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED" ).consume();
            }
        }
        dbRule.restartDatabase( Collections.singletonMap( GraphDatabaseSettings.auth_enabled, true ) );
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );

        // THEN
        DriverAuthHelper.assertAuthFail( boltUri, "foo", "wrongPassword" );
        DriverAuthHelper.assertAuth( boltUri, "foo", "bar" );
    }

    @Test
    public void shouldChangePasswordWithAuthDisabled() throws Exception
    {
        // GIVEN
        try ( Driver driver = connectDriver( boltUri, ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED" ).consume();
            }
        }
        dbRule.restartDatabase( Collections.singletonMap( GraphDatabaseSettings.auth_enabled, false ) );
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );

        try ( Driver driver = connectDriver( boltUri, AuthTokens.none() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                // WHEN
                session.run( "ALTER USER foo SET PASSWORD 'abc' CHANGE NOT REQUIRED" ).consume();
            }
        }
        dbRule.restartDatabase( Collections.singletonMap( GraphDatabaseSettings.auth_enabled, true ) );

        // THEN
        boltUri = DriverAuthHelper.boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );
        DriverAuthHelper.assertAuthFail( boltUri, "foo", "bar" );
        DriverAuthHelper.assertAuth( boltUri, "foo", "abc" );
    }

    private void assertPropertyFunctionWorks( Session session, String user, String key, String query, Function<Record,String> mapper,
            Map<String,List<String>> expectedResults )
    {
        String expectation = "User " + user + " should see " + key + " " + expectedResults.get( key );
        assertThat( expectation, Set.copyOf( session.run( query ).list( mapper ) ), containsInAnyOrder( expectedResults.get( key ).toArray() ) );
    }

    private String labelOf( Record r )
    {
        return fieldOf( r, "label" );
    }

    private String typeOf( Record r )
    {
        return fieldOf( r, "relationshipType" );
    }

    private String propOf( Record r )
    {
        return fieldOf( r, "propertyKey" );
    }

    private String fieldOf( Record r, String key )
    {
        return r.get( key ).toString().replace( "\"", "" );
    }
}
