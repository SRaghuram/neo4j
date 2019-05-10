/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

@RunWith( Parameterized.class )
public class CoreTopologyMarshalTest extends BaseMarshalTest<DatabaseCoreTopology>
{
    public CoreTopologyMarshalTest( DatabaseCoreTopology original )
    {
        super( original, new CoreTopologyMarshal() );
    }

    @Parameterized.Parameters
    public static Collection<DatabaseCoreTopology> data()
    {
        return Arrays.asList(
                new DatabaseCoreTopology( new DatabaseId( "orders" ), new ClusterId( UUID.randomUUID() ), coreServerInfos( 0 ) ),
                new DatabaseCoreTopology( new DatabaseId( "customers" ), new ClusterId( UUID.randomUUID() ), coreServerInfos( 3 ) ),
                new DatabaseCoreTopology( new DatabaseId( "cars" ), null, coreServerInfos( 4 ) )
        );
    }

    public static Map<MemberId,CoreServerInfo> coreServerInfos( int count )
    {
        return IntStream.range( 0, count )
                .mapToObj( i -> Pair.of( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( i, false ) ) )
                .collect( CollectorsUtil.pairsToMap() );
    }
}
