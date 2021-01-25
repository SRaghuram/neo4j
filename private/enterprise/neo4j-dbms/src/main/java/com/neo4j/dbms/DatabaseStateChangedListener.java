/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.dbms.DatabaseState;

/**
 * Simple listener interface for subscribing to databases changing states (e.g. STARTED->STOPPED).
 * Instances implementing this interface should be registered with the {@link DbmsReconciler}.
 *
 * Note: the {@code stateChange()} "event" is only called once for each reconciliation job, once that
 * job has completed. If a database goes through several state transitions in a single job then only the
 * last state will be reported (e.g. for STARTED->STOPPED->DROPPED only DROPPED).
 *
 * Note the second: care should be taken not to block inside implementations of {@code stateChange(...)}
 */
@FunctionalInterface
public interface DatabaseStateChangedListener
{
    void stateChange( DatabaseState previousState, DatabaseState newState );
}
