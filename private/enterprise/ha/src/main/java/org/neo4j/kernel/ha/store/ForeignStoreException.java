/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.store;

import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;

public class ForeignStoreException extends HighAvailabilityStoreFailureException
{
    public ForeignStoreException( StoreId store, StoreId clusterStore )
    {
        super( format( "%s has different origin than the rest of the cluster, %s", store, clusterStore ) );
    }
}
