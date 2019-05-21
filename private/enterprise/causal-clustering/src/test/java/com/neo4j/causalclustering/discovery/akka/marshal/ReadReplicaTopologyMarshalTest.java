/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.neo4j.internal.helpers.collection.CollectorsUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaTopologyMarshalTest extends BaseMarshalTest<DatabaseReadReplicaTopology>
{
    public ReadReplicaTopologyMarshalTest()
    {
        super( generate(), new ReadReplicaTopologyMarshal() );
    }

    static DatabaseReadReplicaTopology generate()
    {
        Map<MemberId,ReadReplicaInfo> replicas = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForReadReplica( id ) ) )
                .collect( CollectorsUtil.pairsToMap() );
        return new DatabaseReadReplicaTopology( new DatabaseId( "hello" ), replicas );
    }

}
