/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.marshal.ChannelMarshal;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class ReadReplicaTopologyMarshalTest implements BaseMarshalTest<DatabaseReadReplicaTopology>
{
    @Override
    public Collection<DatabaseReadReplicaTopology> originals()
    {
        return List.of( generate() );
    }

    @Override
    public ChannelMarshal<DatabaseReadReplicaTopology> marshal()
    {
        return ReadReplicaTopologyMarshal.INSTANCE;
    }

    static DatabaseReadReplicaTopology generate()
    {
        Map<ServerId,ReadReplicaInfo> replicas = IntStream.range( 0, 5 )
                .mapToObj( id -> Pair.of( IdFactory.randomServerId(), TestTopology.addressesForReadReplica( id ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );
        return new DatabaseReadReplicaTopology( randomDatabaseId(), replicas );
    }
}
