/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.routing;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;

class CommercialSingleInstanceRoutingProcedureIT extends CommunitySingleInstanceRoutingProcedureIT
{
    @Override
    protected GraphDatabaseFactory newGraphDatabaseFactory()
    {
        return new TestCommercialGraphDatabaseFactory();
    }
}
