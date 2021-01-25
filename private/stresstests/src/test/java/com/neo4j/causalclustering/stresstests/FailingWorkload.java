/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.helper.Workload;

public class FailingWorkload extends Workload
{
    static final String MESSAGE = "Synthetic failure";

    FailingWorkload( Control control )
    {
        super( control );
    }

    @Override
    protected void doWork()
    {
        throw new RuntimeException( MESSAGE );
    }
}
