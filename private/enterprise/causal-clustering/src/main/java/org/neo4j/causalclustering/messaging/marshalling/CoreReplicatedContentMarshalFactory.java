/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;

public class CoreReplicatedContentMarshalFactory
{
    public static SafeChannelMarshal<ReplicatedContent> marshalV1( String defaultDatabaseName )
    {
        return new CoreReplicatedContentMarshalV1( defaultDatabaseName );
    }

    public static SafeChannelMarshal<ReplicatedContent> marshalV2()
    {
        return new CoreReplicatedContentMarshalV2();
    }

    public static Codec<ReplicatedContent> codecV2()
    {
        return new ReplicatedContentCodec();
    }
}
