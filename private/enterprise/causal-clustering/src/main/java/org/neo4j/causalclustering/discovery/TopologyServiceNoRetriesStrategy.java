/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class TopologyServiceNoRetriesStrategy extends NoRetriesStrategy<MemberId,Optional<AdvertisedSocketAddress>> implements TopologyServiceRetryStrategy
{
}
