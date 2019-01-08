/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime;

/**
 * Creates {@link BoltWorker}s. Implementations of this interface can decorate queues and their jobs
 * to monitor activity and enforce constraints.
 */
public interface WorkerFactory
{
    default BoltWorker newWorker( BoltConnectionDescriptor connectionDescriptor )
    {
        return newWorker( connectionDescriptor, null, null );
    }

    /**
     * @param connectionDescriptor describes the underlying medium (TCP, HTTP, ...)
     * @param queueMonitor         object that will be notified of changes (enqueue, dequeue, drain) in worker job queue
     * @param onClose              callback for closing the underlying connection in case of protocol violation.
     * @return a new job queue
     */
    BoltWorker newWorker( BoltConnectionDescriptor connectionDescriptor, BoltWorkerQueueMonitor queueMonitor, Runnable onClose );
}
