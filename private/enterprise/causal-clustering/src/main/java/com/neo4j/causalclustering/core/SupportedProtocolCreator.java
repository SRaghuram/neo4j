/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.protocol.ApplicationProtocolVersion;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory;
import static java.lang.String.format;

public class SupportedProtocolCreator
{
    private final Config config;
    private final Log log;

    public SupportedProtocolCreator( Config config, LogProvider logProvider )
    {
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    public ApplicationSupportedProtocols getSupportedCatchupProtocolsFromConfiguration()
    {
        return getApplicationSupportedProtocols( config.get( CausalClusteringSettings.catchup_implementations ), ApplicationProtocolCategory.CATCHUP );
    }

    public ApplicationSupportedProtocols getSupportedRaftProtocolsFromConfiguration()
    {
        return getApplicationSupportedProtocols( config.get( CausalClusteringSettings.raft_implementations ), ApplicationProtocolCategory.RAFT );
    }

    private ApplicationSupportedProtocols getApplicationSupportedProtocols( List<ApplicationProtocolVersion> configVersions,
            ApplicationProtocolCategory category )
    {
        if ( configVersions.isEmpty() )
        {
            return new ApplicationSupportedProtocols( category, Collections.emptyList() );
        }
        else
        {
            List<ApplicationProtocolVersion> knownVersions =
                    protocolsForConfig( category, configVersions, version -> ApplicationProtocols.find( category, version ) );
            if ( knownVersions.isEmpty() )
            {
                throw new IllegalArgumentException( format( "None of configured %s implementations %s are known", category.canonicalName(), configVersions ) );
            }
            else
            {
                return new ApplicationSupportedProtocols( category, knownVersions );
            }
        }
    }

    public List<ModifierSupportedProtocols> createSupportedModifierProtocols()
    {
        ModifierSupportedProtocols supportedCompression = compressionProtocolVersions();

        return Stream.of( supportedCompression )
                .filter( supportedProtocols -> !supportedProtocols.versions().isEmpty() )
                .collect( Collectors.toList() );
    }

    private ModifierSupportedProtocols compressionProtocolVersions()
    {
        List<String> implementations = protocolsForConfig( ModifierProtocolCategory.COMPRESSION,
                config.get( CausalClusteringSettings.compression_implementations ),
                implementation -> ModifierProtocols.find( ModifierProtocolCategory.COMPRESSION, implementation ) );

        return new ModifierSupportedProtocols( ModifierProtocolCategory.COMPRESSION, implementations );
    }

    private <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> List<IMPL> protocolsForConfig( Protocol.Category<T> category, List<IMPL> implementations,
            Function<IMPL,Optional<T>> finder )
    {
        return implementations.stream()
                .map( impl -> Pair.of( impl, finder.apply( impl ) ) )
                .peek( protocolWithImplementation -> logUnknownProtocol( category, protocolWithImplementation ) )
                .map( Pair::other )
                .flatMap( Optional::stream )
                .map( Protocol::implementation )
                .collect( Collectors.toList() );
    }

    private <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> void logUnknownProtocol( Protocol.Category<T> category,
            Pair<IMPL,Optional<T>> protocolWithImplementation )
    {
        if ( protocolWithImplementation.other().isEmpty() )
        {
            log.warn( "Configured %s protocol implementation %s unknown. Ignoring.", category, protocolWithImplementation.first() );
        }
    }
}
