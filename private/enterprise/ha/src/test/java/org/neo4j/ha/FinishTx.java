/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

public class FinishTx implements WorkerCommand<HighlyAvailableGraphDatabase, Void>
{
    private final Transaction tx;
    private final boolean successful;

    public FinishTx( Transaction tx, boolean successful )
    {
        this.tx = tx;
        this.successful = successful;
    }

    @Override
    public Void doWork( HighlyAvailableGraphDatabase state )
    {
        if ( successful )
        {
            tx.success();
        }
        tx.close();
        return null;
    }
}
