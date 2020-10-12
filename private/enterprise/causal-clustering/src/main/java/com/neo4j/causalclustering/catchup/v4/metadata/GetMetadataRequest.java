/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.metadata;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

public class GetMetadataRequest extends CatchupProtocolMessage
{
    public final String databaseName;
    public final IncludeMetadata includeMetadata;

    public GetMetadataRequest( String databaseName, String includeMetadata )
    {
        super( RequestMessageType.METADATA_REQUEST );
        this.databaseName = databaseName;
        this.includeMetadata = IncludeMetadata.valueOf( includeMetadata );
    }

    @Override
    public String describe()
    {
        return getClass().getSimpleName();
    }
}
