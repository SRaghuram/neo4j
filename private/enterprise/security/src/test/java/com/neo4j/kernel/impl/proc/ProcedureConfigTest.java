/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.proc;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.procedure.impl.ProcedureConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.default_allowed;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_roles;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;


class ProcedureConfigTest
{
    private static final String[] EMPTY = new String[]{};

    private static String[] arrayOf( String... values )
    {
        return values;
    }

    @Test
    void shouldHaveEmptyDefaultConfigs()
    {
        Config config = Config.defaults();
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "x" ) ).isEqualTo( EMPTY );
    }

    @Test
    void shouldHaveConfigsWithDefaultProcedureAllowed()
    {
        Config config = Config.defaults( default_allowed, "role1" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "x" ) ).isEqualTo( arrayOf( "role1" ) );
    }

    @Test
    void shouldHaveConfigsWithExactMatchProcedureAllowed()
    {
        Config config = Config.defaults( Map.of( default_allowed, "role1",
                procedure_roles, "xyz:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ) ).isEqualTo( arrayOf( "anotherRole" ) );
        assertThat( procConfig.rolesFor( "abc" ) ).isEqualTo( arrayOf( "role1" ) );
    }

    @Test
    void shouldNotFailOnEmptyStringDefaultName()
    {
        Config config = Config.defaults( default_allowed, "" );
        new ProcedureConfig( config );
    }

    @Test
    void shouldNotFailOnEmptyStringRoles()
    {
        Config config = Config.defaults( procedure_roles, "" );
        new ProcedureConfig( config );
    }

    @Test
    void shouldNotFailOnBadStringRoles()
    {
        Config config = Config.defaults( procedure_roles, "matrix" );
        new ProcedureConfig( config );
    }

    @Test
    void shouldNotFailOnEmptyStringBoth()
    {
        Config config = Config.defaults( Map.of( default_allowed, "",
                        procedure_roles, "" ) );
        new ProcedureConfig( config );
    }

    @Test
    void shouldHaveConfigsWithWildcardProcedureAllowed()
    {
        Config config = Config.defaults( Map.of( default_allowed, "role1", procedure_roles,
                        "xyz*:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ) ).isEqualTo( arrayOf( "anotherRole" ) );
        assertThat( procConfig.rolesFor( "abcxyz" ) ).isEqualTo( arrayOf( "role1" ) );
    }

    @Test
    void shouldHaveConfigsWithWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults( procedure_roles, "xyz*:anotherRole" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ) ).isEqualTo( arrayOf( "anotherRole" ) );
        assertThat( procConfig.rolesFor( "abcxyz" ) ).isEqualTo( EMPTY );
    }

    @Test
    void shouldHaveConfigsWithMultipleWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults( procedure_roles,
                "apoc.convert.*:apoc_reader;apoc.load.json:apoc_writer;apoc.trigger.add:TriggerHappy" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ) ).isEqualTo( EMPTY );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ) ).isEqualTo( arrayOf( "apoc_reader" ) );
        assertThat( procConfig.rolesFor( "apoc.convert.json" ) ).isEqualTo( arrayOf( "apoc_reader" ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ) ).isEqualTo( EMPTY );
        assertThat( procConfig.rolesFor( "apoc.load.json" ) ).isEqualTo( arrayOf( "apoc_writer" ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ) ).isEqualTo( arrayOf( "TriggerHappy" ) );
        assertThat( procConfig.rolesFor( "apoc.convert-json" ) ).isEqualTo( EMPTY );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ) ).isEqualTo( EMPTY );
        assertThat( procConfig.rolesFor( "apoc.load-json" ) ).isEqualTo( EMPTY );
        assertThat( procConfig.rolesFor( "apoc.trigger-add" ) ).isEqualTo( EMPTY );
    }

    @Test
    void shouldHaveConfigsWithOverlappingMatchingWildcards()
    {
        Config config = Config.defaults( Map.of( default_allowed, "default", procedure_roles,
                        "apoc.*:apoc;apoc.load.*:loader;apoc.trigger.*:trigger;apoc.trigger.add:TriggerHappy" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ) ).isEqualTo( arrayOf( "default" ) );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ) ).isEqualTo( arrayOf( "apoc" ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ) ).isEqualTo( arrayOf( "apoc", "loader" ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ) ).isEqualTo( arrayOf( "apoc", "trigger", "TriggerHappy" ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.remove" ) ).isEqualTo( arrayOf( "apoc", "trigger" ) );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ) ).isEqualTo( arrayOf( "apoc" ) );
    }

    @Test
    void shouldSupportSeveralRolesPerPattern()
    {
        Config config = Config.defaults( procedure_roles,
                "xyz*:role1,role2,  role3  ,    role4   ;    abc:  role3   ,role1" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ) ).isEqualTo( arrayOf( "role1", "role2", "role3", "role4" ) );
        assertThat( procConfig.rolesFor( "abc" ) ).isEqualTo( arrayOf( "role3", "role1" ) );
        assertThat( procConfig.rolesFor( "abcxyz" ) ).isEqualTo( EMPTY );
    }

    @Test
    void shouldNotAllowFullAccessDefault()
    {
        Config config = Config.defaults();
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.fullAccessFor( "x" ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowFullAccessForProcedures()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test.procedure.name" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowFullAccessForSeveralProcedures()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test.procedure.name", "test.procedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowFullAccessForSeveralProceduresOddNames()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test\\.procedure.name", "test*rocedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.fullAccessFor( "test\\.procedure.name" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test*procedure.otherName" ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowFullAccessWildcardProceduresNames()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( " test.procedure.*  ", "     test.*.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test.other.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test.other.cool.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.fullAccessFor( "test.other.name" ) ).isEqualTo( false );
    }

    @Test
    void shouldBlockWithWhiteListingForProcedures()
    {
        Config config = Config.defaults( Map.of(
                procedure_unrestricted, List.of( "test.procedure.name", "test.procedure.name2" ),
                procedure_allowlist, List.of( "test.procedure.name" ) ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ) ).isEqualTo( true );
        assertThat( procConfig.isWhitelisted( "test.procedure.name2" ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowWhiteListsWildcardProceduresNames()
    {
        Config config = Config.defaults( procedure_allowlist, List.of( " test.procedure.*",  "test.*.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ) ).isEqualTo( true );
        assertThat( procConfig.isWhitelisted( "test.procedure.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.isWhitelisted( "test.other.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.isWhitelisted( "test.other.cool.otherName" ) ).isEqualTo( true );
        assertThat( procConfig.isWhitelisted( "test.other.name" ) ).isEqualTo( false );
    }

    @Test
    void shouldIgnoreOddRegex()
    {
        Config config = Config.defaults( procedure_allowlist, List.of( "[\\db^a]*" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "123" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "b" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "a" ) ).isEqualTo( false );

        config = Config.defaults( procedure_allowlist, List.of( "(abc)" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "(abc)" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "^$" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "^$" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "\\" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "\\" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "&&" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "&&" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "\\p{Lower}" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "\\p{Lower}" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "a+" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "aaaaaa" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "a+" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "a|b" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "b" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "|" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "a|b" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "[a-c]" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "b" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "c" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "-" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "[a-c]" ) ).isEqualTo( true );

        config = Config.defaults( procedure_allowlist, List.of( "a\tb" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a    b" ) ).isEqualTo( false );
        assertThat( procConfig.isWhitelisted( "a\tb" ) ).isEqualTo( true );
    }
}
