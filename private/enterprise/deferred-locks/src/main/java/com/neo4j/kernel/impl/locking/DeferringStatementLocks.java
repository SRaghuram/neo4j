/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.lock.LockTracer;

/**
 * A {@link StatementLocks} implementation that defers {@link #optimistic() optimistic}
 * locks using {@link DeferringLockClient}.
 */
public class DeferringStatementLocks implements StatementLocks
{
    private final Locks.Client explicit;
    private final DeferringLockClient implicit;

    public DeferringStatementLocks( Locks.Client explicit )
    {
        this.explicit = explicit;
        this.implicit = new DeferringLockClient( this.explicit );
    }

    @Override
    public void initialize( LeaseClient leaseClient, long userTransactionId )
    {
        explicit.initialize( leaseClient, userTransactionId );
    }

    @Override
    public Locks.Client pessimistic()
    {
        return explicit;
    }

    @Override
    public Locks.Client optimistic()
    {
        return implicit;
    }

    @Override
    public void prepareForCommit( LockTracer lockTracer )
    {
        implicit.acquireDeferredLocks( lockTracer );
        explicit.prepare();
    }

    @Override
    public void stop()
    {
        implicit.stop();
    }

    @Override
    public void close()
    {
        implicit.close();
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return Stream.concat( explicit.activeLocks(), implicit.activeLocks() );
    }

    @Override
    public long activeLockCount()
    {
        return explicit.activeLockCount() + implicit.activeLockCount();
    }
}
