/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
 * Normally, a {@link CoreDownloaderService} uses this internal operator to transition a database
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
 * bootstrapped, or as ready for restarting after a store copy.
 */
public class ClusterInternalDbmsOperator extends DbmsOperator
{
    private final List<StoreCopyHandle> storeCopying;
    private final Set<NamedDatabaseId> bootstrapping;
    private final Set<NamedDatabaseId> shouldStop;
    private final Log log;

    public ClusterInternalDbmsOperator( LogProvider logProvider )
    {
        this.storeCopying = new CopyOnWriteArrayList<>();
        this.bootstrapping = ConcurrentHashMap.newKeySet();
        this.shouldStop = ConcurrentHashMap.newKeySet();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        var result = new HashMap<String,EnterpriseDatabaseState>();

        for ( var storeCopyHandle : storeCopying )
        {
            var id = storeCopyHandle.namedDatabaseId;
            if ( !bootstrapping.contains( id ) )
            {
                result.put( id.name(), new EnterpriseDatabaseState( id, STORE_COPYING ) );
            }
        }

        for ( var id : shouldStop )
        {
            result.put( id.name(), new EnterpriseDatabaseState( id, STOPPED ) );
        }

        return result;
    }

    /**
     * Unlike {@link ClusterInternalDbmsOperator#bootstrap(NamedDatabaseId)}, this method will explicitly trigger the reconciler,
     * and block until the database in question has explicitly transitioned to {@link EnterpriseOperatorState#STORE_COPYING}.
     *
     * The one exception to this blocking behaviour is if the operator currently also desires a database to be in a
     * bootstrapping state. To block in this circumstance would cause a deadlock, as a bootstrapping database *also*
     * performs a store copy, and waits for its completion.
     *
     * @param namedDatabaseId the id of the database to be store copied
     * @return a handle which can be used to signal the completion of a store copy.
     */
    public StoreCopyHandle stopForStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        StoreCopyHandle storeCopyHandle = new StoreCopyHandle( this, namedDatabaseId );
        storeCopying.add( storeCopyHandle );
        triggerReconcilerOnStoreCopy( namedDatabaseId );
        return storeCopyHandle;
    }

    public void stopOnPanic( NamedDatabaseId namedDatabaseId, Throwable causeOfPanic )
    {
        if ( causeOfPanic == null )
        {
            log.warn( "Panic event received for %s but the provided cause is null, so this event was ignored.", namedDatabaseId );
            return;
        }

        shouldStop.add( namedDatabaseId );
        var reconcilerResult = trigger( ReconcilerRequest.panickedTarget( namedDatabaseId, causeOfPanic ).build() );
        reconcilerResult.whenComplete( () -> shouldStop.remove( namedDatabaseId ) );
    }

    private boolean triggerReconcilerOnStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        if ( bootstrapping.contains( namedDatabaseId ) || shouldStop.contains( namedDatabaseId ) )
        {
            return false;
        }
        trigger( ReconcilerRequest.simple() ).await( namedDatabaseId );
        return true;
    }

    /**
     * Prevents state transitions to the STORE_COPYING state while startup is on-going, because the reconciler
     * is already in the process of reconciling to the STARTED state and the {@link CoreDownloaderService}
     * which is a component used both during startup and while the database is up and running will ask for a
     * transition to STORE_COPYING. Trying to "switch" the reconciler to STORE_COPYING would currently deadlock it.
     * This is a cooperative design between the reconciler and the startup code, which might seem a bit awkward.
     *
     * Note that unlike {@link ClusterInternalDbmsOperator#stopForStoreCopy(NamedDatabaseId)} this operation does not
     * trigger the reconciler, and is not blocking. Instead it simply serves as a marker for any other operators which
     * may trigger the reconciler in parallel, signalling that this database is bootstrapping.
     *
     * @param namedDatabaseId the id of the database being bootstrapped
     * @return a handle that must be released when bootstrapping is done.
     */
    public BootstrappingHandle bootstrap( NamedDatabaseId namedDatabaseId )
    {
        bootstrapping.add( namedDatabaseId );
        return new BootstrappingHandle( this, namedDatabaseId );
    }

    public static class StoreCopyHandle
    {
        private final ClusterInternalDbmsOperator operator;
        private final NamedDatabaseId namedDatabaseId;

        private StoreCopyHandle( ClusterInternalDbmsOperator operator, NamedDatabaseId namedDatabaseId )
        {
            this.operator = operator;
            this.namedDatabaseId = namedDatabaseId;
        }

        /**
         * @return true if the reconciler was triggered, otherwise false.
         */
        public boolean release()
        {
            boolean exists = operator.storeCopying.remove( this );
            if ( !exists )
            {
                throw new IllegalStateException( "Restart was already called for " + namedDatabaseId );
            }

            return operator.triggerReconcilerOnStoreCopy( namedDatabaseId );
        }

        public NamedDatabaseId databaseId()
        {
            return namedDatabaseId;
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
            return Objects.equals( namedDatabaseId, that.namedDatabaseId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( namedDatabaseId );
        }
    }

    public static class BootstrappingHandle
    {
        private final ClusterInternalDbmsOperator operator;
        private final NamedDatabaseId namedDatabaseId;

        private BootstrappingHandle( ClusterInternalDbmsOperator operator, NamedDatabaseId namedDatabaseId )
        {
            this.operator = operator;
            this.namedDatabaseId = namedDatabaseId;
        }

        public void release()
        {
            if ( !operator.bootstrapping.remove( namedDatabaseId ) )
            {
                throw new IllegalStateException( "Bootstrapped was already called for " + namedDatabaseId );
            }
        }
    }
}
