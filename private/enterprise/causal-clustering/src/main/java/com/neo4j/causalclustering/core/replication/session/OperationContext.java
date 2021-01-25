/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

/** Context for operation. Used for acquirement and release. */
public class OperationContext
{
    private final GlobalSession globalSession;
    private final LocalOperationId localOperationId;

    private final LocalSession localSession;

    public OperationContext( GlobalSession globalSession, LocalOperationId localOperationId, LocalSession localSession )
    {
        this.globalSession = globalSession;
        this.localOperationId = localOperationId;
        this.localSession = localSession;
    }

    public GlobalSession globalSession()
    {
        return globalSession;
    }

    public LocalOperationId localOperationId()
    {
        return localOperationId;
    }

    protected LocalSession localSession()
    {
        return localSession;
    }

    @Override
    public String toString()
    {
        return "OperationContext{" +
               "globalSession=" + globalSession +
               ", localOperationId=" + localOperationId +
               '}';
    }
}
