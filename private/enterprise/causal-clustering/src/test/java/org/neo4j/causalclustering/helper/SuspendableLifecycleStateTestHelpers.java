/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.lifecycle.Lifecycle;

class SuspendableLifecycleStateTestHelpers
{
    static void setInitialState( StateAwareSuspendableLifeCycle lifeCycle, LifeCycleState state ) throws Throwable
    {
        for ( LifeCycleState lifeCycleState : LifeCycleState.values() )
        {
            if ( lifeCycleState.compareTo( state ) <= 0 )
            {
                lifeCycleState.set( lifeCycle );
            }
        }
    }

    enum LifeCycleState
    {
        Init( Lifecycle::init ),
        Start( Lifecycle::start ),
        Stop( Lifecycle::stop ),
        Shutdown( Lifecycle::shutdown );

        private final ThrowingConsumer<Lifecycle,Throwable> operation;

        LifeCycleState( ThrowingConsumer<Lifecycle,Throwable> operation )
        {
            this.operation = operation;
        }

        void set( Lifecycle lifecycle ) throws Throwable
        {
            operation.accept( lifecycle );
        }
    }

    enum SuspendedState
    {
        Untouched( suspendable -> {} ),
        Enabled( Suspendable::enable ),
        Disabled( Suspendable::disable );

        private final ThrowingConsumer<Suspendable,Throwable> consumer;

        SuspendedState( ThrowingConsumer<Suspendable,Throwable> consumer )
        {
            this.consumer = consumer;
        }

        void set( Suspendable suspendable ) throws Throwable
        {
            consumer.accept( suspendable );
        }
    }
}
