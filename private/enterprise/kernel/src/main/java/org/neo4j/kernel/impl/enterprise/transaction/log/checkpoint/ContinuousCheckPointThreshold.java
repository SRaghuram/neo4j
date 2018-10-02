/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.kernel.impl.transaction.log.checkpoint.ReachedThreshold;

class ContinuousCheckPointThreshold extends ReachedThreshold
{
    public static final ContinuousCheckPointThreshold INSTANCE = new ContinuousCheckPointThreshold();

    private ContinuousCheckPointThreshold()
    {
        super( "continuous threshold" );
    }
}