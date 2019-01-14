/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2;

import org.neo4j.causalclustering.catchup.Protocol;

public class ContentTypeProtocol extends Protocol<ContentType>
{
    public ContentTypeProtocol()
    {
        super( ContentType.ContentType );
    }
}
