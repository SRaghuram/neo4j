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
package org.neo4j.kernel.impl.enterprise;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;

import static java.util.stream.Collectors.toSet;

public class StandardBoltConnectionTracker implements BoltConnectionTracker
{
    private Map<ManagedBoltStateMachine,String> sessions = new ConcurrentHashMap<>();

    @Override
    public void onRegister( ManagedBoltStateMachine machine, String owner )
    {
        sessions.put( machine, owner );
    }

    @Override
    public void onTerminate( ManagedBoltStateMachine machine )
    {
        sessions.remove( machine );
    }

    @Override
    public Set<ManagedBoltStateMachine> getActiveConnections()
    {
        return sessions.keySet().stream().collect( toSet() );
    }

    @Override
    public Set<ManagedBoltStateMachine> getActiveConnections( String owner )
    {
        return sessions
                .entrySet()
                .stream()
                .filter( entry -> entry.getValue().equals( owner ) )
                .map( Map.Entry::getKey ).collect( toSet() );
    }
}
