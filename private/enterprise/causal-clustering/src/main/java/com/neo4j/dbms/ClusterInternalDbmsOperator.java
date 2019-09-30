/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;

/**
 * Database operator for Clustered databases exposing state transitions needed by internal components.
 *
 * Specifically, this operator allows components to mark databases as {@link EnterpriseOperatorState#STORE_COPYING}
 * or bootstrapping.
 *
 * {@link EnterpriseOperatorState#STORE_COPYING} refers to the state in which an underlying kernel database is
 * unavailable whilst the * cluster machinery downloads a more recent copy of store files and
 * transactions from another * cluster member.
 *
 * Normally, a {@link PersistentSnapshotDownloader} uses this internal operator to transition a database
 * from {@link EnterpriseOperatorState#STARTED}, to {@link EnterpriseOperatorState#STORE_COPYING}. However, during the
 * transition of a database from null to {@link EnterpriseOperatorState#STARTED}, such an attempt will lead to a
 * deadlock. As a result, bootstrapping is not a concrete {@link EnterpriseOperatorState}, provided by this operator
 * to the {@link DbmsReconciler}. Instead, the bootstrapping {@code Set<DatabaseId>} field contains
 * transient flags, rendering an attempt to stop a database for store copy a no-op, if it is currently
 * bootstrapping.
 *
 * In other words, these two states are mutually exclusive, and in the event of overlap, bootstrap
 * will take priority.
 *
 * Note that this operator imposes the strict requirement that calling components must transition
 * databases back to their original state when they are done: marking databases as successfully
 * boostrapped, or as ready for restarting after a store copy.
 */
public final class ClusterInternalDbmsOperator extends DbmsOperator
{
    private final List<StoreCopyHandle> storeCopying = new CopyOnWriteArrayList<>();
    private final Set<DatabaseId> bootstrapping = ConcurrentHashMap.newKeySet();
    private final Set<DatabaseId> panicked = ConcurrentHashMap.newKeySet();

    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        var result = new HashMap<String,EnterpriseDatabaseState>();

        for ( var storeCopyHandle : storeCopying )
        {
            var id = storeCopyHandle.databaseId;
            if ( !bootstrapping.contains( id ) )
            {
                result.put( id.name(), new EnterpriseDatabaseState( id, STORE_COPYING ) );
            }
        }

        for ( var id : panicked )
        {
            result.put( id.name(), new EnterpriseDatabaseState( id, STOPPED ) );
        }

        return result;
    }

    /**
     * Unlike {@link ClusterInternalDbmsOperator#bootstrap(DatabaseId)}, this method will explicitly trigger the reconciler,
     * and block until the database in question has explicitly transitioned to {@link EnterpriseOperatorState#STORE_COPYING}.
     *
     * The one exception to this blocking behaviour is if the operator currently also desires a database to be in a
     * bootstrapping state. To block in this circumstance would cause a deadlock, as a bootstrapping database *also*
     * performs a store copy, and waits for its completion.
     *
     * @param databaseId the id of the database to be store copied
     * @return a handle which can be used to signal the completion of a store copy.
     */
    public StoreCopyHandle stopForStoreCopy( DatabaseId databaseId )
    {
        StoreCopyHandle storeCopyHandle = new StoreCopyHandle( this, databaseId );
        storeCopying.add( storeCopyHandle );
        triggerReconcilerOnStoreCopy( databaseId );
        return storeCopyHandle;
    }

    public void stopOnPanic( DatabaseId databaseId, Throwable causeOfPanic )
    {
        Objects.requireNonNull( causeOfPanic, "The cause of a panic cannot be null!" );
        panicked.add( databaseId );
        var reconcilerResult = trigger( ReconcilerRequest.forPanickedDatabase( databaseId, causeOfPanic ) );
        reconcilerResult.whenComplete( () -> panicked.remove( databaseId ) );
    }

    private boolean triggerReconcilerOnStoreCopy( DatabaseId databaseId )
    {
        if ( !bootstrapping.contains( databaseId ) && !panicked.contains( databaseId ) )
        {
            trigger( ReconcilerRequest.simple() ).await( databaseId );
            return true;
        }
        return false;
    }

    /**
     * Note that unlike {@link ClusterInternalDbmsOperator#stopForStoreCopy(DatabaseId)} this operation does not trigger
     * the reconciler, and is not blocking. Instead it simply serves as a marker for any other operators which
     * make trigger the reconciler in parallel, signalling that this database is bootstrapping.
     *
     * @param databaseId the id of the database being bootstrapped
     * @return a handle which can be used to signal bootstrap completion
     */
    public BootstrappingHandle bootstrap( DatabaseId databaseId )
    {
        bootstrapping.add( databaseId );
        return new BootstrappingHandle( this, databaseId );
    }

    public static class StoreCopyHandle
    {
        private final ClusterInternalDbmsOperator operator;
        private final DatabaseId databaseId;

        private StoreCopyHandle( ClusterInternalDbmsOperator operator, DatabaseId databaseId )
        {
            this.operator = operator;
            this.databaseId = databaseId;
        }

        /**
         * Starts the database again, unless we're currently in the bootstrapping phase.
         *
         * @return true if the database was started, otherwise false.
         */
        public boolean release()
        {
            boolean exists = operator.storeCopying.remove( this );
            if ( !exists )
            {
                throw new IllegalStateException( "Restart was already called for " + databaseId );
            }

            return operator.triggerReconcilerOnStoreCopy( databaseId );
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

    public static class BootstrappingHandle
    {
        private final ClusterInternalDbmsOperator operator;
        private final DatabaseId databaseId;

        private BootstrappingHandle( ClusterInternalDbmsOperator operator, DatabaseId databaseId )
        {
            this.operator = operator;
            this.databaseId = databaseId;
        }

        public void bootstrapped()
        {
            if ( !operator.bootstrapping.remove( databaseId ) )
            {
                throw new IllegalStateException( "Bootstrapped was already called for " + databaseId );
            }
        }
    }
}
