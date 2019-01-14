/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.collection.Streams;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;

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
        return getApplicationSupportedProtocols( config.get( CausalClusteringSettings.catchup_implementations ), CATCHUP );
    }

    public ApplicationSupportedProtocols getSupportedRaftProtocolsFromConfiguration()
    {
        return getApplicationSupportedProtocols( config.get( CausalClusteringSettings.raft_implementations ), RAFT );
    }

    public ApplicationSupportedProtocols getAllCatchupProtocols()
    {
        return new ApplicationSupportedProtocols( CATCHUP, Collections.emptyList() );
    }

    public ApplicationSupportedProtocols getAllRaftProtocols()
    {
        return new ApplicationSupportedProtocols( RAFT, Collections.emptyList() );
    }

    public ApplicationSupportedProtocols getMinimumCatchupProtocols( int minimumVersion )
    {
        return filterProtocols( minimumVersion, CATCHUP );
    }

    public ApplicationSupportedProtocols getMinimumRaftProtocols( int minimumVersion )
    {
        return filterProtocols( minimumVersion, RAFT );
    }

    private ApplicationSupportedProtocols filterProtocols( int minimumVersion, Protocol.ApplicationProtocolCategory category )
    {
        List<Integer> protocolVersions = ApplicationProtocols.filterByVersion( category, version -> version >= minimumVersion )
                .stream().map( Protocol::implementation ).collect( Collectors.toList() );

        if ( protocolVersions.isEmpty() )
        {
            throw new IllegalStateException(
                    format( "No known implementations of the %s protocol have a version >= %d", category.canonicalName(), minimumVersion ) );
        }

        return new ApplicationSupportedProtocols( category, protocolVersions );
    }

    private ApplicationSupportedProtocols getApplicationSupportedProtocols( List<Integer> configVersions, Protocol.ApplicationProtocolCategory category )
    {
        if ( configVersions.isEmpty() )
        {
            return new ApplicationSupportedProtocols( category, Collections.emptyList() );
        }
        else
        {
            List<Integer> knownVersions = protocolsForConfig( category, configVersions, version -> ApplicationProtocols.find( category, version ) );
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
        List<String> implementations = protocolsForConfig( COMPRESSION, config.get( CausalClusteringSettings.compression_implementations ),
                implementation -> Protocol.ModifierProtocols.find( COMPRESSION, implementation ) );

        return new ModifierSupportedProtocols( COMPRESSION, implementations );
    }

    private <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> List<IMPL> protocolsForConfig( Protocol.Category<T> category, List<IMPL> implementations,
            Function<IMPL,Optional<T>> finder )
    {
        return implementations.stream()
                .map( impl -> Pair.of( impl, finder.apply( impl ) ) )
                .peek( protocolWithImplementation -> logUnknownProtocol( category, protocolWithImplementation ) )
                .map( Pair::other )
                .flatMap( Streams::ofOptional )
                .map( Protocol::implementation )
                .collect( Collectors.toList() );
    }

    private <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> void logUnknownProtocol( Protocol.Category<T> category,
            Pair<IMPL,Optional<T>> protocolWithImplementation )
    {
        if ( !protocolWithImplementation.other().isPresent() )
        {
            log.warn( "Configured %s protocol implementation %s unknown. Ignoring.", category, protocolWithImplementation.first() );
        }
    }
}
