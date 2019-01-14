/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import org.neo4j.helper.Workload;

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
