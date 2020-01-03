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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.default_allowed;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_roles;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_whitelist;


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
        assertThat( procConfig.rolesFor( "x" ), equalTo( EMPTY ) );
    }

    @Test
    void shouldHaveConfigsWithDefaultProcedureAllowed()
    {
        Config config = Config.defaults( default_allowed, "role1" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "x" ), equalTo( arrayOf( "role1" ) ) );
    }

    @Test
    void shouldHaveConfigsWithExactMatchProcedureAllowed()
    {
        Config config = Config.defaults( Map.of( default_allowed, "role1",
                procedure_roles, "xyz:anotherRole" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abc" ), equalTo( arrayOf( "role1" ) ) );
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
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( arrayOf( "role1" ) ) );
    }

    @Test
    void shouldHaveConfigsWithWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults( procedure_roles, "xyz*:anotherRole" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "anotherRole" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( EMPTY ) );
    }

    @Test
    void shouldHaveConfigsWithMultipleWildcardProcedureAllowedAndNoDefault()
    {
        Config config = Config.defaults( procedure_roles,
                "apoc.convert.*:apoc_reader;apoc.load.json:apoc_writer;apoc.trigger.add:TriggerHappy" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ), equalTo( arrayOf( "apoc_reader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert.json" ), equalTo( arrayOf( "apoc_reader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load.json" ), equalTo( arrayOf( "apoc_writer" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ), equalTo( arrayOf( "TriggerHappy" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert-json" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.load-json" ), equalTo( EMPTY ) );
        assertThat( procConfig.rolesFor( "apoc.trigger-add" ), equalTo( EMPTY ) );
    }

    @Test
    void shouldHaveConfigsWithOverlappingMatchingWildcards()
    {
        Config config = Config.defaults( Map.of( default_allowed, "default", procedure_roles,
                        "apoc.*:apoc;apoc.load.*:loader;apoc.trigger.*:trigger;apoc.trigger.add:TriggerHappy" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyz" ), equalTo( arrayOf( "default" ) ) );
        assertThat( procConfig.rolesFor( "apoc.convert.xml" ), equalTo( arrayOf( "apoc" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load.xml" ), equalTo( arrayOf( "apoc", "loader" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.add" ), equalTo( arrayOf( "apoc", "trigger", "TriggerHappy" ) ) );
        assertThat( procConfig.rolesFor( "apoc.trigger.remove" ), equalTo( arrayOf( "apoc", "trigger" ) ) );
        assertThat( procConfig.rolesFor( "apoc.load-xml" ), equalTo( arrayOf( "apoc" ) ) );
    }

    @Test
    void shouldSupportSeveralRolesPerPattern()
    {
        Config config = Config.defaults( procedure_roles,
                "xyz*:role1,role2,  role3  ,    role4   ;    abc:  role3   ,role1" );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.rolesFor( "xyzabc" ), equalTo( arrayOf( "role1", "role2", "role3", "role4" ) ) );
        assertThat( procConfig.rolesFor( "abc" ), equalTo( arrayOf( "role3", "role1" ) ) );
        assertThat( procConfig.rolesFor( "abcxyz" ), equalTo( EMPTY ) );
    }

    @Test
    void shouldNotAllowFullAccessDefault()
    {
        Config config = Config.defaults();
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.fullAccessFor( "x" ), equalTo( false ) );
    }

    @Test
    void shouldAllowFullAccessForProcedures()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test.procedure.name" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
    }

    @Test
    void shouldAllowFullAccessForSeveralProcedures()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test.procedure.name", "test.procedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ), equalTo( true ) );
    }

    @Test
    void shouldAllowFullAccessForSeveralProceduresOddNames()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( "test\\.procedure.name", "test*rocedure.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test\\.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test*procedure.otherName" ), equalTo( true ) );
    }

    @Test
    void shouldAllowFullAccessWildcardProceduresNames()
    {
        Config config = Config.defaults( procedure_unrestricted, List.of( " test.procedure.*  ", "     test.*.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.fullAccessFor( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.procedure.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.cool.otherName" ), equalTo( true ) );
        assertThat( procConfig.fullAccessFor( "test.other.name" ), equalTo( false ) );
    }

    @Test
    void shouldBlockWithWhiteListingForProcedures()
    {
        Config config = Config.defaults( Map.of(
                procedure_unrestricted, List.of( "test.procedure.name", "test.procedure.name2" ),
                procedure_whitelist, List.of( "test.procedure.name" ) ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name2" ), equalTo( false ) );
    }

    @Test
    void shouldAllowWhiteListsWildcardProceduresNames()
    {
        Config config = Config.defaults( procedure_whitelist, List.of( " test.procedure.*",  "test.*.otherName" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );

        assertThat( procConfig.isWhitelisted( "xyzabc" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.name" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.procedure.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.cool.otherName" ), equalTo( true ) );
        assertThat( procConfig.isWhitelisted( "test.other.name" ), equalTo( false ) );
    }

    @Test
    void shouldIgnoreOddRegex()
    {
        Config config = Config.defaults( procedure_whitelist, List.of( "[\\db^a]*" ) );
        ProcedureConfig procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "123" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );

        config = Config.defaults( procedure_whitelist, List.of( "(abc)" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "(abc)" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "^$" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "^$" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "\\" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "\\" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "&&" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "&&" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "\\p{Lower}" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "\\p{Lower}" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "a+" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "aaaaaa" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a+" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "a|b" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "|" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a|b" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "[a-c]" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "c" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "-" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "[a-c]" ), equalTo( true ) );

        config = Config.defaults( procedure_whitelist, List.of( "a\tb" ) );
        procConfig = new ProcedureConfig( config );
        assertThat( procConfig.isWhitelisted( "a    b" ), equalTo( false ) );
        assertThat( procConfig.isWhitelisted( "a\tb" ), equalTo( true ) );
    }
}
