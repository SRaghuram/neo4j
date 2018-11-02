/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorContext;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.LogProvider;

/**
 * Context for the {@link AcceptorState} distributed state machine.
 * <p/>
 * This holds the store for Paxos instances, as seen from the acceptor role point of view in Paxos.
 */
class AcceptorContextImpl
        extends AbstractContextImpl
        implements AcceptorContext
{
    private final AcceptorInstanceStore instanceStore;

    AcceptorContextImpl( org.neo4j.cluster.InstanceId me, CommonContextState commonState,
            LogProvider logging,
            Timeouts timeouts, AcceptorInstanceStore instanceStore )
    {
        super( me, commonState, logging, timeouts );
        this.instanceStore = instanceStore;
    }

    @Override
    public AcceptorInstance getAcceptorInstance( InstanceId instanceId )
    {
        return instanceStore.getAcceptorInstance( instanceId );
    }

    @Override
    public void promise( AcceptorInstance instance, long ballot )
    {
        instanceStore.promise( instance, ballot );
    }

    @Override
    public void accept( AcceptorInstance instance, Object value )
    {
        instanceStore.accept( instance, value );
    }

    @Override
    public void leave()
    {
        instanceStore.clear();
    }

    public AcceptorContextImpl snapshot( CommonContextState commonStateSnapshot, LogProvider logging, Timeouts timeouts,
                                         AcceptorInstanceStore instanceStore )
    {
        return new AcceptorContextImpl( me, commonStateSnapshot, logging, timeouts, instanceStore );
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

        AcceptorContextImpl that = (AcceptorContextImpl) o;

        return instanceStore != null ? instanceStore.equals( that.instanceStore ) : that.instanceStore == null;
    }

    @Override
    public int hashCode()
    {
        return instanceStore != null ? instanceStore.hashCode() : 0;
    }
}
