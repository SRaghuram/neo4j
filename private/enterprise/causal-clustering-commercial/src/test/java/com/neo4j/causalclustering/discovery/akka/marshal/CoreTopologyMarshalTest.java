/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TestTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;

public class CoreTopologyMarshalTest extends BaseMarshalTest<CoreTopology>
{
    public CoreTopologyMarshalTest()
    {
        super( new CoreTopology( new ClusterId( UUID.randomUUID() ), true, CoreTopologyMarshalTest.coreServerInfos( 3 ) ),
               new CoreTopologyMarshal() );
    }

    public static Map<MemberId,CoreServerInfo> coreServerInfos( int count )
    {
        return IntStream.range( 0, count )
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( CollectorsUtil.pairsToMap() );
    }
}
