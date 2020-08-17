/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ApplicationProtocolUtil
{
    public static void checkInstallersExhaustive( Set<ApplicationProtocol> keySet, ApplicationProtocolCategory category )
    {
        final var raftProtocolVersions = List.of( ApplicationProtocols.values() ).stream().filter( v -> v.category().equals( category.name() ) ).count();
        if ( raftProtocolVersions > keySet.size() )
        {
            throw new IllegalStateException( "There is unregistered client installer or server installer" );
        }
    }

    public static List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>>
    buildServerInstallers( Map<ApplicationProtocol,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolMap,
                           ApplicationProtocol maximumProtocol )
    {
        return protocolMap
                .entrySet()
                .stream()
                .filter( p -> p.getKey().lessOrEquals( maximumProtocol ) )
                .map( Map.Entry::getValue )
                .collect( Collectors.toList() );
    }

    public static List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>>
    buildClientInstallers( Map<ApplicationProtocol,ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Client,?>> protocolMap,
            ApplicationProtocol maximumProtocol )
    {
        return protocolMap
                .entrySet()
                .stream()
                .filter( p -> p.getKey().lessOrEquals( maximumProtocol ) )
                .map( Map.Entry::getValue )
                .collect( Collectors.toList() );
    }
}
