/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.statemachine;

/**
 * Listener API for StateMachine transitions. Emitted on each
 * message handled by the state machine.
 */
public interface StateTransitionListener
{
    void stateTransition( StateTransition transition );
}
