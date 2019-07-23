/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.CommercialAuthAndUserManager;
import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.config.Setting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.SessionConfig.forDatabase;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class NativeAuthIT extends EnterpriseAuthenticationTestBase
{
    private static final String READ_USER = "neo";
    private static final String WRITE_USER = "tank";
    private static final String ADMIN_USER = "neo4j";

    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        return Map.of(
                SecuritySettings.authentication_providers, SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers, SecuritySettings.NATIVE_REALM_NAME
        );
    }

    private final String password = "secret";

    protected String getPassword()
    {
        return password;
    }

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        CommercialAuthAndUserManager authManager = dbRule.resolveDependency( CommercialAuthAndUserManager.class );
        EnterpriseUserManager userManager = authManager.getUserManager();
        userManager.newUser( READ_USER, password.getBytes(), false );
        userManager.newUser( WRITE_USER, password.getBytes(), false );
        userManager.setUserPassword( ADMIN_USER, password.getBytes(), false );
        userManager.addRoleToUser( PredefinedRoles.READER, READ_USER );
        userManager.addRoleToUser( PredefinedRoles.PUBLISHER, WRITE_USER );
    }

    @Test
    public void shouldNotFailOnUpdatingSecurityCommandsOnSystemDb()
    {
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE DATABASE foo" ).consume();
                session.run( "CREATE ROLE fooRole" ).consume();
                session.run( "CALL dbms.security.createUser('fooUser', 'fooPassword')" ).consume();
                session.run( "GRANT MATCH (foo) ON GRAPH * NODES * TO fooRole" ).consume();
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
        try ( Driver driver = connectDriver( WRITE_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( DEFAULT_DATABASE_NAME ) ) )
            {
                session.run( "CREATE (:A)-[:X]->(:B)<-[:Y]-(:C)" ).consume();
                session.run( "MATCH (n:A) SET n.prop1 = 1" ).consume();
                session.run( "MATCH (n:B) SET n.prop2 = 2" ).consume();
                session.run( "MATCH (n:C) SET n.prop3 = 3" ).consume();
            }
        }

        try ( Driver driver = connectDriver( READ_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( DEFAULT_DATABASE_NAME ) ) )
            {
                assertThat( session.run( "CALL db.labels" ).list( this::labelOf ), equalTo( List.of( "A", "B", "C" ) ) );
                assertThat( session.run( "CALL db.relationshipTypes" ).list( this::typeOf ), equalTo( List.of( "X", "Y" ) ) );
                assertThat( session.run( "CALL db.propertyKeys" ).list( this::propOf ), equalTo( List.of( "prop1", "prop2", "prop3" ) ) );
            }
        }
    }

    @Test
    public void shouldNotFailOnDatabaseSchemaProceduresWithRestrictedUserOnNewDatabase()
    {
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE DATABASE foo" ).consume();
                session.run( "CREATE ROLE custom" ).consume();
                session.run( "CREATE USER joe SET PASSWORD $password CHANGE NOT REQUIRED", map( "password", getPassword() ) ).consume();
                session.run( "GRANT ROLE custom TO joe", map( "password", getPassword() ) ).consume();
                session.run( "GRANT MATCH (prop1) ON GRAPH * NODES * TO custom" ).consume();
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
            }
        }

        try ( Driver driver = connectDriver( WRITE_USER, getPassword() ) )
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
                        "joe", Map.of( "labels", List.of( "B", "C" ), "types", List.of( "Y" ), "props", List.of( "prop1", "prop2", "prop3" ) ) );
        for ( String user : userExpectedResults.keySet() )
        {
            Map<String,List<String>> expectedResults = userExpectedResults.get( user );
            try ( Driver driver = connectDriver( user, getPassword() ) )
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
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE USER tim SET PASSWORD $password CHANGE NOT REQUIRED", map( "password", getPassword() ) ).consume();
                session.run( "CREATE ROLE role" ).consume();
                session.run( "GRANT ROLE role TO tim" ).consume();
                session.run( "GRANT MATCH(*) ON GRAPH * ELEMENTS * TO role" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH * NODES City TO role" ).consume();
                session.run( "DENY TRAVERSE ON GRAPH * RELATIONSHIPS FROM TO role" ).consume();
                session.run( "DENY READ(name) ON GRAPH * NODES Person TO role" ).consume();
                session.run( "CREATE DATABASE foo" ).consume();
            }
            try ( Session session = driver.session( forDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW ROLE role PRIVILEGES" ).list();
                assertThat( "Should have the right number of underlying privileges", records.size(), equalTo( 7 ) );
                List<Record> grants = records.stream().filter( r -> {
                    Map<String, Object> m = r.asMap();
                    return m.get( "grant" ).equals( "DENIED" ) && m.get( "resource" ).equals( "property(name)" ) && m.get( "segment" ).equals( "NODE(Person)" );
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

        try ( Driver driver = connectDriver( WRITE_USER, getPassword() ) )
        {
            try ( Session session = driver.session( forDatabase( "foo" ) ) )
            {
                session.run( "CREATE (:PostCode {name:'postcode'})-[:LOCATED_IN]->(:City {name:'city'})-[:BELONGS_TO]->(:State {name:'state'})" ).consume();
                for ( String label : labels )
                {
                    if ( !label.equals( "City" ) )
                    {
                        for ( String relType : types )
                        {
                            if ( !relType.equals( "LOCATED_IN" ) && !relType.equals( "BELONGS_TO" ) )
                            {
                                session.run( "CREATE (:" + label + " {name:'" + label + "'})-[:" + relType + "]->(:" + label + ")" ).consume();
                            }
                        }
                    }
                }
            }
        }

        List<String> timLabels = labels.stream().filter( v -> !v.equals( "City" ) ).collect( Collectors.toList() );
        List<String> timTypes =
                types.stream().filter( v -> !(v.equals( "LOCATED_IN" ) || v.equals( "BELONGS_TO" ) || v.equals( "FROM" )) ).collect( Collectors.toList() );
        Map<String,Map<String,List<String>>> userExpectedResults =
                Map.of( READ_USER, Map.of( "labels", labels, "types", types, "props", List.of( "name" ) ),
                        "tim", Map.of( "labels", timLabels, "types", timTypes, "props", List.of( "name" ) ) );
        for ( String user : userExpectedResults.keySet() )
        {
            Map<String,List<String>> expectedResults = userExpectedResults.get( user );
            try ( Driver driver = connectDriver( user, getPassword() ) )
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

    private void assertPropertyFunctionWorks( Session session, String user, String key, String query, Function<Record,String> mapper,
            Map<String,List<String>> expectedResults )
    {
        String expectation = "User " + user + " should see " + key + " " + expectedResults.get( key );
        assertThat( expectation, Set.copyOf( session.run( query ).list( mapper ) ), equalTo( Set.copyOf( expectedResults.get( key ) ) ) );
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
