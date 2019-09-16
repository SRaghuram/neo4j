/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnterpriseDbmsExtension
class SchedulerProceduresTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldListActiveGroups()
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( "CALL dbms.scheduler.groups" ) )
            {
                assertTrue( result.hasNext() );
                while ( result.hasNext() )
                {
                    assertThat( (Long) result.next().get( "threads" ), greaterThan( 0L ) );
                }
            }
            tx.commit();
        }
    }

    @Test
    void shouldProfileGroup()
    {
        try ( Transaction tx = db.beginTx() )
        {
            String result = tx.execute( "CALL dbms.scheduler.profile('sample', 'CypherWorker', '5s')" ).resultAsString();
            assertThat( result, containsString( "morsel.Worker.run" ) );
            tx.commit();
        }
    }
}
