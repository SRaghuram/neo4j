/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;

public class CypherQueriesIT extends CommercialVersionIT
{

    @Test
    public void runningInCompiledRuntime() throws Exception
    {
        // Given
        String uri = functionalTestHelper.dataUri() + "transaction/commit";
        String payload = "{ 'statements': [ { 'statement': 'CYPHER runtime=compiled MATCH (n) RETURN n' } ] }";

        // When
        HTTP.Response res = HTTP.POST(uri, payload.replaceAll("'", "\""));

        // Then
        assertEquals( 200, res.status() );
    }
}
