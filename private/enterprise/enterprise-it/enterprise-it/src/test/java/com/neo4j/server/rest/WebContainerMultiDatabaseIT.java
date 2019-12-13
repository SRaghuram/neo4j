/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class WebContainerMultiDatabaseIT extends EnterpriseWebContainerIT
{
    @Test
    public void shouldBeAbleToCreateAndDropDatabases()
    {
        HTTP.Response res = HTTP.POST( functionalTestHelper.txCommitUri( "system" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE DATABASE foo' } ] }" ) );
        assertEquals( 200, res.status() );
        assertThat( res, containsNoErrors() );

        HTTP.Response resRun = HTTP.POST( functionalTestHelper.txCommitUri( "foo" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n: Node) RETURN n' } ] }" ) );
        assertEquals( 200, resRun.status() );
        assertThat( resRun, containsNoErrors() );

        HTTP.Response resDrop = HTTP.POST( functionalTestHelper.txCommitUri( "system" ),
                quotedJson( "{ 'statements': [ { 'statement': 'DROP DATABASE foo' } ] }" ) );
        assertEquals( 200, resDrop.status() );
        assertThat( resDrop, containsNoErrors() );

        HTTP.Response resRunAfterDrop = HTTP.POST( functionalTestHelper.txCommitUri( "foo" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE {n: Node} RETURN n' } ] }" ) );
        assertEquals( 404, resRunAfterDrop.status() );
        assertThat( resRunAfterDrop, hasErrors( Status.Database.DatabaseNotFound ) );
    }
}
