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
                privilegeFor( "role", "*", true )
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
                privilegeFor( "role", "*", true ),
                privilegeFor( "role", "xyz", false ),
                privilegeFor( "anotherRole", "xyz", true )
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
                privilegeFor( "role", "*", true ),
                privilegeFor( "role", "x?z*", false ),
                privilegeFor( "anotherRole", "x?z*", true )
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
                privilegeFor( "role", "xyz*", true )
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
                privilegeFor( "apoc_reader", "apoc.convert.*", true ),
                privilegeFor( "apoc_writer", "apoc.load.json", true ),
                privilegeFor( "TriggerHappy", "apoc.trigger.add", true )
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
                privilegeFor( "role1", "xyz*", true ),
                privilegeFor( "role2", "xyz*", true ),
                privilegeFor( "role3", "xyz*", true ),
                privilegeFor( "role4", "xyz*", true ),
                privilegeFor( "role1", "abc", true ),
                privilegeFor( "role3", "abc", true )
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
                privilegeFor( "apoc", "apoc.*", true ),
                privilegeFor( "loader", "apoc.load.*", true ),
                privilegeFor( "trigger", "apoc.trigger.*", true ),

                privilegeFor( "default", "apoc.*", false ),
                privilegeFor( "default", "apoc.load.*", true ),
                privilegeFor( "default", "apoc.trigger.*", false ),
                privilegeFor( "default", "*", true )
        );
    }

    Map<String,String> privilegeFor( String role, String procedure, boolean granted )
    {
        return Map.of( "role", role,
                "graph", "*",
                "segment", String.format( "PROCEDURE(%s)", procedure ),
                "resource", "database",
                "action", EXECUTE_BOOSTED_FROM_CONFIG,
                "access", granted ? "GRANTED" : "DENIED" );
    }
}
