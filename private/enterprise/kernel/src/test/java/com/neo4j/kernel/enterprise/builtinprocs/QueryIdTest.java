/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static com.neo4j.kernel.enterprise.builtinprocs.QueryId.fromExternalString;
import static com.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class QueryIdTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void printsQueryIds() throws InvalidArgumentsException
    {
        assertThat( ofInternalId( 12L ).toString(), equalTo( "query-12" ) );
    }

    @Test
    public void doesNotConstructNegativeQueryIds() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        ofInternalId( -15L );
    }

    @Test
    public void parsesQueryIds() throws InvalidArgumentsException
    {
        assertThat( fromExternalString( "query-14" ), equalTo( ofInternalId( 14L ) ) );
    }

    @Test
    public void doesNotParseNegativeQueryIds() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "query--12" );
    }

    @Test
    public void doesNotParseRandomText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "blarglbarf" );
    }

    @Test
    public void doesNotParseTrailingRandomText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "query-12  " );
    }

    @Test
    public void doesNotParseEmptyText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "" );
    }
}
