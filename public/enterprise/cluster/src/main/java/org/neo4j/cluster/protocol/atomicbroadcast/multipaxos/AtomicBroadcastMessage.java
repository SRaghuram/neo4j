/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.com.message.MessageType;

/**
 * Messages for the AtomicBroadcast client API. These correspond to the methods in {@link org.neo4j.cluster.protocol
 * .atomicbroadcast.AtomicBroadcast}
 * as well as internal messages for implementing the AB protocol.
 */
public enum AtomicBroadcastMessage
        implements MessageType
{
    // AtomicBroadcast API messages
    broadcast, addAtomicBroadcastListener, removeAtomicBroadcastListener,

    // Protocol implementation messages
    entered, join, leave, // Group management
    broadcastResponse, broadcastTimeout, failed // Internal message created by implementation
}
