/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.STORE_COPYING;

/**
 * Database operator for Clustered databases exposing state transitions needed by internal components.
 *
 * Specifically, this operator allows components to mark databases as STORE_COPYING or BOOTSTRAPPING.
 *
 * STORE_COPYING refers to the state in which an underlying kernel database is unavailable whilst the
 * cluster machinery downloads a more recent copy of store files and transactions from another
 * cluster member.
 *
 * Normally, a {@link PersistentSnapshotDownloader} uses this internal operator to transition a
 * database from STARTED, to STORE_COPYING. However, during the transition of a database from null
 * to STARTED, such an attempt will lead to a deadlock. As a result, BOOTSTRAPPING is not a concrete
 * {@link OperatorState}, provided by this operator to the {@link DbmsReconciler}. Instead,
 * the bootstrapping {@code Set<DatabaseId>} field contains transient flags, rendering an attempt to
 * stop a database for store copy a no-op, if it is currently bootstrapping.
 *
 * In other words, these two states are mutually exclusive, and in the event of overlap,
 * BOOTSTRAP will take priority.
 *
 * Note that this operator imposes the strict requirement that calling components must transition
 * databases back to their original state when they are done: marking databases as successfully
 * boostrapped, or as ready for restarting after a store copy.
 */
public class ClusterInternalDbmsOperator implements DbmsOperator
{
    private final List<StoreCopyHandle> storeCopying = new CopyOnWriteArrayList<>();
    private final Set<DatabaseId> bootstrapping = ConcurrentHashMap.newKeySet();

    private OperatorConnector connector;

    @Override
    public void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return storeCopying.stream()
                .filter( handle -> !bootstrapping.contains( handle.databaseId ) )
                .distinct()
                .collect( Collectors.toMap( StoreCopyHandle::databaseId, ignored -> STORE_COPYING ) );
    }

    /**
     * Unlike {@link ClusterInternalDbmsOperator#bootstrap(DatabaseId)}, this method will explicitly trigger the reconciler,
     * and block until the database in question has explicitly transitioned to STORE_COPYING.
     *
     * The one exception to this blocking behaviour is if the operator currently also desires a database to be in a
     * BOOTSTRAPPING state. To block in this circumstance would cause a deadlock, as a BOOTSTRAPPING database *also*
     * performs a store copy, and waits for its completion.
     *
     * @param databaseId the id of the database to be store copied
     * @return a handle which can be used to signal the completion of a store copy.
     */
    public synchronized StoreCopyHandle stopForStoreCopy( DatabaseId databaseId )
    {
        StoreCopyHandle storeCopyHandle = new StoreCopyHandle( databaseId );
        storeCopying.add( storeCopyHandle );

        if ( !bootstrapping.contains( databaseId ) )
        {
            trigger().await( databaseId );
        }
        return storeCopyHandle;
    }

    /**
     * Note that unlike {@link ClusterInternalDbmsOperator#stopForStoreCopy(DatabaseId)} this operation does not trigger
     * the reconciler, and is not blocking. Instead it simply serves as a marker for any other operators which
     * make trigger the reconciler in parallel, signalling that this database is BOOTSTRAPPING.
     *
     * @param databaseId the id of the database being bootstrapped
     * @return a handle which can be used to signal bootstrap completion
     */
    public synchronized BootstrappingHandle bootstrap( DatabaseId databaseId )
    {
        bootstrapping.add( databaseId );
        return new BootstrappingHandle( databaseId );
    }

    private Reconciliation trigger()
    {
        if ( connector == null )
        {
            return Reconciliation.EMPTY;
        }
        return connector.trigger();
    }

    public class StoreCopyHandle
    {
        private final DatabaseId databaseId;

        private StoreCopyHandle( DatabaseId databaseId )
        {
            this.databaseId = databaseId;
        }

        public void restart()
        {
            boolean exists = storeCopying.remove( this );
            if ( !exists )
            {
                throw new IllegalStateException( "Restart was already called for " + databaseId );
            }

            if ( !bootstrapping.contains( databaseId ) )
            {
                trigger().await( databaseId );
            }
        }

        public DatabaseId databaseId()
        {
            return databaseId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            StoreCopyHandle that = (StoreCopyHandle) o;
            return Objects.equals( databaseId, that.databaseId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( databaseId );
        }
    }

    public class BootstrappingHandle
    {
        private final DatabaseId databaseId;

        private BootstrappingHandle( DatabaseId databaseId )
        {
            this.databaseId = databaseId;
        }

        public void bootstrapped()
        {
            if ( !bootstrapping.remove( databaseId ) )
            {
                throw new IllegalStateException( "Bootstrapped was already called for " + databaseId );
            }
        }
    }
}
