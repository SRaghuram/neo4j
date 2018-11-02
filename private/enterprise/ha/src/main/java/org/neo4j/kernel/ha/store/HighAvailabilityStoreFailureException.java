/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.store;

import org.neo4j.kernel.impl.store.StoreFailureException;

/**
 * Represents a serious, show-stopping error that most likely signals that a member cannot participate in a cluster.
 */
public abstract class HighAvailabilityStoreFailureException extends StoreFailureException
{
    protected HighAvailabilityStoreFailureException( String msg )
    {
        super( msg );
    }
}
