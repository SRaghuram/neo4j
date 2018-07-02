/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.neo4j.util.concurrent.BinaryLatch;

/**
 * This is used to signal in the {@link FulltextProviderImpl} that index flipping should stop.
 */
class FlipperStopSignal extends BinaryLatch
{
    FlipperStopSignal()
    {
        release();
    }
}
