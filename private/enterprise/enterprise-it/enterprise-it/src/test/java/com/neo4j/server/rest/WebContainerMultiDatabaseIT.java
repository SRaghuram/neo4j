/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class WebContainerMultiDatabaseIT extends EnterpriseWebContainerIT
{
    @Test
    public void shouldBeAbleToCreateAndDropDatabases()
    {
        HTTP.Response res = HTTP.POST( functionalTestHelper.txCommitUri( "system" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE DATABASE foo' } ] }" ) );
        assertEquals( 200, res.status() );
        assertThat( res ).satisfies( containsNoErrors() );

        HTTP.Response resRun = HTTP.POST( functionalTestHelper.txCommitUri( "foo" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n: Node) RETURN n' } ] }" ) );
        assertEquals( 200, resRun.status() );
        assertThat( resRun ).satisfies( containsNoErrors() );

        HTTP.Response resDrop = HTTP.POST( functionalTestHelper.txCommitUri( "system" ),
                quotedJson( "{ 'statements': [ { 'statement': 'DROP DATABASE foo' } ] }" ) );
        assertEquals( 200, resDrop.status() );
        assertThat( resDrop ).satisfies( containsNoErrors() );

        HTTP.Response resRunAfterDrop = HTTP.POST( functionalTestHelper.txCommitUri( "foo" ),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE {n: Node} RETURN n' } ] }" ) );
        assertEquals( 404, resRunAfterDrop.status() );
        assertThat( resRunAfterDrop ).satisfies( hasErrors( Status.Database.DatabaseNotFound ) );
    }
}
