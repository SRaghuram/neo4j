/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

public class BeginTx implements WorkerCommand<HighlyAvailableGraphDatabase, Transaction>
{
    @Override
    public Transaction doWork( HighlyAvailableGraphDatabase state )
    {
        return state.beginTx();
    }
}
