/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.commercial.builtin;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CommercialDbmsExtension
class SchedulerProceduresTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldListActiveGroups()
    {
        try ( Result result = db.execute( "CALL dbms.scheduler.groups" ) )
        {
            assertTrue( result.hasNext() );
            while ( result.hasNext() )
            {
                assertThat( (Long) result.next().get( "threads" ), Matchers.greaterThan( 0L ) );
            }
        }
    }
}
