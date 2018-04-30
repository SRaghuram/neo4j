/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;

public interface CommercialDiscoveryServiceType
{
    Supplier<DiscoveryServiceFactory> AKKA = CommercialAkkaDiscoveryServiceFactory::new;

    List<Supplier<DiscoveryServiceFactory>> values = Arrays.asList( AKKA );
}
