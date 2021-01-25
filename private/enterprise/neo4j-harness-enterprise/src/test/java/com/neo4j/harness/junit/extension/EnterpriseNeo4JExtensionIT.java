/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( EnterpriseNeo4jExtension.class )
class EnterpriseNeo4JExtensionIT
{
    @BeforeEach
    void setUp( Neo4j neo4j, GraphDatabaseService databaseService )
    {
        assertNotNull( neo4j );
        assertNotNull( databaseService );
    }

    @Test
    void neo4jAvailable( Neo4j neo4j )
    {
        assertNotNull( neo4j );
        assertThat( HTTP.GET( neo4j.httpURI().toString() ).status() ).isEqualTo( 200 );
    }

    @Test
    void graphDatabaseServiceIsAvailable( GraphDatabaseService databaseService )
    {
        assertNotNull( databaseService );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
    }
}
