/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.statemachine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Handle for dynamic proxies that are backed by a {@link StateMachine}.
 * Delegates calls to a {@link StateMachineProxyFactory}, which in turn
 * will call the {@link StateMachine}.
 */
public class StateMachineProxyHandler
        implements InvocationHandler
{
    private StateMachineProxyFactory stateMachineProxyFactory;
    private StateMachine<?,?> stateMachine;

    public StateMachineProxyHandler( StateMachineProxyFactory stateMachineProxyFactory, StateMachine<?,?> stateMachine )
    {
        this.stateMachineProxyFactory = stateMachineProxyFactory;
        this.stateMachine = stateMachine;
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args )
    {
        // Delegate call to factory, which will translate method call into state machine invocation
        return stateMachineProxyFactory.invoke( stateMachine, method, args == null ? null : (args.length > 1 ? args :
                args[0]) );
    }

    public StateMachineProxyFactory getStateMachineProxyFactory()
    {
        return stateMachineProxyFactory;
    }
}
