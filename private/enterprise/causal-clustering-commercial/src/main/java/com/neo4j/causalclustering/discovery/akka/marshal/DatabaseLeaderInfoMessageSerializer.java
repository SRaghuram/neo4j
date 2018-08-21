/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

public class DatabaseLeaderInfoMessageSerializer extends BaseAkkaSerializer<LeaderInfoDirectoryMessage>
{
    protected DatabaseLeaderInfoMessageSerializer()
    {
        super( new DatabaseLeaderInfoMessageMarshal(), BaseAkkaSerializer.DB_LEADER_INFO, 256 );
    }
}
