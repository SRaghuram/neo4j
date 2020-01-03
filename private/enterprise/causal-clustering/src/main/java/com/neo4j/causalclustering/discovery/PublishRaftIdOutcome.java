/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

public enum PublishRaftIdOutcome
{
    FAILED_PUBLISH,
    SUCCESSFUL_PUBLISH_BY_ME,
    SUCCESSFUL_PUBLISH_BY_OTHER,
}
