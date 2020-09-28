/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

import static com.neo4j.server.security.enterprise.auth.PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PrivilegeResolverTest
{
    private SystemGraphRealm systemGraphRealm;

    @BeforeEach
    void setup()
    {
        systemGraphRealm = mock( SystemGraphRealm.class );
    }

    @Test
    void shouldHaveNoTemporaryPrivilegesWithDefaultConfig()
    {
        // WHEN
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, Config.defaults() );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).isEmpty();
    }

    @Test
    void shouldHaveNoTemporaryPrivilegesWithEmptyConfig()
    {
        // WHEN
        Config config = Config.defaults( Map.of(
                GraphDatabaseSettings.default_allowed, "",
                GraphDatabaseSettings.procedure_roles, "" )
        );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).isEmpty();
    }

    @Test
    void shouldHaveBoostAllWithDefaultProcedureAllowed()
    {
        // WHEN
        Config config = Config.defaults( GraphDatabaseSettings.default_allowed, "role" );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "role", "*", true ),
                functionPrivilegeFor( "role", "*", true )
        );
    }

    @Test
    void shouldHaveBoostWithExactMatchProcedureAllowed()
    {
        // WHEN
        Config config = Config.defaults( Map.of(
                GraphDatabaseSettings.default_allowed, "role",
                GraphDatabaseSettings.procedure_roles, "xyz:anotherRole" )
        );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "role", "*", true ),
                procedurePrivilegeFor( "role", "xyz", false ),
                procedurePrivilegeFor( "anotherRole", "xyz", true ),
                functionPrivilegeFor( "role", "*", true ),
                functionPrivilegeFor( "role", "xyz", false ),
                functionPrivilegeFor( "anotherRole", "xyz", true )
        );
    }

    @Test
    void shouldHaveConfigsWithWildcardProcedureAllowed()
    {
        // WHEN
        Config config = Config.defaults( Map.of(
                GraphDatabaseSettings.default_allowed, "role",
                GraphDatabaseSettings.procedure_roles, "x?z*:anotherRole" )
        );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "role", "*", true ),
                procedurePrivilegeFor( "role", "x?z*", false ),
                procedurePrivilegeFor( "anotherRole", "x?z*", true ),
                functionPrivilegeFor( "role", "*", true ),
                functionPrivilegeFor( "role", "x?z*", false ),
                functionPrivilegeFor( "anotherRole", "x?z*", true )
        );
    }

    @Test
    void shouldHaveConfigsWithWildcardProcedureAllowedAndNoDefault()
    {
        // WHEN
        Config config = Config.defaults( GraphDatabaseSettings.procedure_roles, "xyz*:role" );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "role", "xyz*", true ),
                functionPrivilegeFor( "role", "xyz*", true )
        );
    }

    @Test
    void shouldNotFailOnBadStringRoles()
    {
        // WHEN
        Config config = Config.defaults( GraphDatabaseSettings.procedure_roles, "matrix" );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).isEmpty();
    }

    @Test
    void shouldHaveConfigsWithMultipleWildcardProcedureAllowedAndNoDefault()
    {
        // WHEN
        Config config = Config.defaults( GraphDatabaseSettings.procedure_roles,
                "apoc.convert.*:apoc_reader;apoc.load.json:apoc_writer;apoc.trigger.add:TriggerHappy" );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "apoc_reader", "apoc.convert.*", true ),
                procedurePrivilegeFor( "apoc_writer", "apoc.load.json", true ),
                procedurePrivilegeFor( "TriggerHappy", "apoc.trigger.add", true ),
                functionPrivilegeFor( "apoc_reader", "apoc.convert.*", true ),
                functionPrivilegeFor( "apoc_writer", "apoc.load.json", true ),
                functionPrivilegeFor( "TriggerHappy", "apoc.trigger.add", true )
        );
    }

    @Test
    void shouldSupportSeveralRolesPerPattern()
    {
        // WHEN
        Config config = Config.defaults( GraphDatabaseSettings.procedure_roles, "xyz*:role1,role2,  role3  ,    role4   ;    abc:  role3   ,role1" );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "role1", "xyz*", true ),
                procedurePrivilegeFor( "role2", "xyz*", true ),
                procedurePrivilegeFor( "role3", "xyz*", true ),
                procedurePrivilegeFor( "role4", "xyz*", true ),
                procedurePrivilegeFor( "role1", "abc", true ),
                procedurePrivilegeFor( "role3", "abc", true ),
                functionPrivilegeFor( "role1", "xyz*", true ),
                functionPrivilegeFor( "role2", "xyz*", true ),
                functionPrivilegeFor( "role3", "xyz*", true ),
                functionPrivilegeFor( "role4", "xyz*", true ),
                functionPrivilegeFor( "role1", "abc", true ),
                functionPrivilegeFor( "role3", "abc", true )
        );
    }

    @Test
    void shouldHaveConfigsWithDefaultInPattern()
    {
        // WHEN
        Config config = Config.defaults( Map.of(
                GraphDatabaseSettings.default_allowed, "default",
                GraphDatabaseSettings.procedure_roles, "apoc.*:apoc;apoc.load.*:loader,default;apoc.trigger.*:trigger" )
        );
        PrivilegeResolver privilegeResolver = new PrivilegeResolver( systemGraphRealm, config );

        // THEN
        assertThat( privilegeResolver.getPrivilegesGrantedThroughConfig() ).containsExactlyInAnyOrder(
                procedurePrivilegeFor( "apoc", "apoc.*", true ),
                procedurePrivilegeFor( "loader", "apoc.load.*", true ),
                procedurePrivilegeFor( "trigger", "apoc.trigger.*", true ),

                procedurePrivilegeFor( "default", "apoc.*", false ),
                procedurePrivilegeFor( "default", "apoc.load.*", true ),
                procedurePrivilegeFor( "default", "apoc.trigger.*", false ),
                procedurePrivilegeFor( "default", "*", true ),

                functionPrivilegeFor( "apoc", "apoc.*", true ),
                functionPrivilegeFor( "loader", "apoc.load.*", true ),
                functionPrivilegeFor( "trigger", "apoc.trigger.*", true ),

                functionPrivilegeFor( "default", "apoc.*", false ),
                functionPrivilegeFor( "default", "apoc.load.*", true ),
                functionPrivilegeFor( "default", "apoc.trigger.*", false ),
                functionPrivilegeFor( "default", "*", true )
        );
    }

    Map<String,String> procedurePrivilegeFor( String role, String procedure, boolean granted )
    {
        return Map.of( "role", role,
                "graph", "*",
                "segment", String.format( "PROCEDURE(%s)", procedure ),
                "resource", "database",
                "action", EXECUTE_BOOSTED_FROM_CONFIG,
                "access", granted ? "GRANTED" : "DENIED" );
    }

    Map<String,String> functionPrivilegeFor( String role, String procedure, boolean granted )
    {
        return Map.of( "role", role,
                "graph", "*",
                "segment", String.format( "FUNCTION(%s)", procedure ),
                "resource", "database",
                "action", EXECUTE_BOOSTED_FROM_CONFIG,
                "access", granted ? "GRANTED" : "DENIED" );
    }
}
