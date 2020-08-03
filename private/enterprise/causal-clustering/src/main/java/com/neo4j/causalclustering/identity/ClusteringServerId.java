/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import java.util.UUID;

import org.neo4j.dbms.identity.StandaloneServerId;

class ClusteringServerId extends StandaloneServerId implements MemberId
{
    ClusteringServerId( UUID uuid )
    {
        super( uuid );
    }
}
