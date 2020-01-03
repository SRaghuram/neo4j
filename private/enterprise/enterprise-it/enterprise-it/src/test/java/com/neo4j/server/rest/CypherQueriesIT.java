/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class CypherQueriesIT extends EnterpriseWebContainerIT
{
    @Test
    public void runningInCompiledRuntime()
    {
        // Given
        String uri = functionalTestHelper.txCommitUri();
        String payload = "{ 'statements': [ { 'statement': 'CYPHER runtime=legacy_compiled MATCH (n) RETURN n' } ] }";

        // When
        HTTP.Response res = HTTP.POST( uri, quotedJson( payload ) );

        // Then
        assertEquals( 200, res.status() );
    }
}
