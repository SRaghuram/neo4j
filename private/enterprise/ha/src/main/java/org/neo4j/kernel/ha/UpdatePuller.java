/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

/**
 * Puller of transactions updates from a different store. Pulls for updates and applies them into a current store.
 * <p>
 * On a running instance of a store there should be only one active implementation of this interface.
 * <p>
 *
 * @see SlaveUpdatePuller
 * @see MasterUpdatePuller
 */
public interface UpdatePuller
{
    /**
     * Pull all available updates.
     *
     * @throws InterruptedException in case if interrupted while waiting for updates
     */
    void pullUpdates() throws InterruptedException;

    /**
     * Try to pull all updates
     *
     * @return true if all updates pulled, false if updater fail on update retrieval
     * @throws InterruptedException in case if interrupted while waiting for updates
     */
    boolean tryPullUpdates() throws InterruptedException;

    /**
     * Start update pulling
     */
    void start();

    /**
     * Terminate update pulling
     */
    void stop();

    /**
     * Pull updates and waits for the supplied condition to be
     * fulfilled as part of the update pulling happening.
     *
     * @param condition {@link UpdatePuller.Condition} to wait for.
     * @param assertPullerActive if {@code true} then observing an inactive update puller
     * will throw an {@link IllegalStateException},
     * @throws InterruptedException if we were interrupted while awaiting the condition.
     * @throws IllegalStateException if {@code strictlyAssertActive} and the update puller
     * became inactive while awaiting the condition.
     */
    void pullUpdates( Condition condition, boolean assertPullerActive ) throws InterruptedException;

    /**
     * Condition to be meet during update pulling.
     */
    interface Condition
    {
        boolean evaluate( int currentTicket, int targetTicket );
    }

}
