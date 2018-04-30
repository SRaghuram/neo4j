/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.TestTopology;

public class CoreServerInfoMarshalTest extends BaseMarshalTest<CoreServerInfo>
{
    public CoreServerInfoMarshalTest()
    {
        super( TestTopology.addressesForCore( 237, false ), new CoreServerInfoMarshal() );
    }
}
