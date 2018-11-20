/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.EnterpriseDbmsRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public class CompiledRuntimeEchoIT
{
    @Rule
    public final EnterpriseDbmsRule db = new EnterpriseDbmsRule();

    @Test
    public void shouldBeAbleToEchoMaps()
    {
        echo( map( "foo", "bar" ) );
        echo( map( "foo", 42L ) );
        echo( map( "foo", map( "bar", map( "baz", 1337L ) ) ) );
    }

    @Test
    public void shouldBeAbleToEchoLists()
    {
        echo( asList( 1L, 2L, 3L ) );
        echo( asList( "a", 1L, 17L ) );
        echo( map( "foo", asList( asList( 1L, 2L, 3L ), "foo" ) ) );
    }

    @Test
    public void shouldBeAbleToEchoListsOfMaps()
    {
        echo( singletonList( map( "foo", "bar" ) ) );
        echo( asList( "a", 1L, 17L, map( "foo", asList( 1L, 2L, 3L ) ) ) );
        echo( asList( "foo", asList( map( "bar", 42L ), "foo" ) ) );
    }

    @Test
    public void shouldBeAbleToEchoMapsOfLists()
    {
        echo( map( "foo", singletonList( "bar" ) ) );
        echo( map( "foo", singletonList( map( "bar", map( "baz", 1337L ) ) ) ) );
    }

    private void echo( Object value )
    {
        Object result = db.execute( "CYPHER runtime=compiled RETURN {p} AS p", map( "p", value ) ).next().get( "p" );
        assertThat( result, equalTo( value ) );
    }
}
