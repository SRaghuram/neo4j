/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.neo4j.causalclustering.discovery.DnsHostnameResolver;
import com.neo4j.causalclustering.discovery.DomainNameResolverImpl;
import com.neo4j.causalclustering.discovery.KubernetesResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.SrvHostnameResolver;
import com.neo4j.causalclustering.discovery.SrvRecordResolverImpl;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.internal.LogService;

import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.configuration.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.configuration.CausalClusteringSettings.kubernetes_service_port_name;

@PublicApi
public enum DiscoveryType
{
    DNS( ( logService, conf ) -> DnsHostnameResolver.resolver( logService, new DomainNameResolverImpl(), conf ),
            initial_discovery_members ),

    LIST( ( logService, conf ) -> NoOpHostnameResolver.resolver( conf ),
            initial_discovery_members ),

    SRV( ( logService, conf ) -> SrvHostnameResolver.resolver( logService, new SrvRecordResolverImpl(), conf ),
            initial_discovery_members ),

    K8S( KubernetesResolver::resolver,
            kubernetes_label_selector, kubernetes_service_port_name );

    private final BiFunction<LogService,Config,RemoteMembersResolver> resolverSupplier;
    private final Collection<Setting<?>> requiredSettings;

    DiscoveryType( BiFunction<LogService,Config,RemoteMembersResolver> resolverSupplier, Setting<?>... requiredSettings )
    {
        this.resolverSupplier = resolverSupplier;
        this.requiredSettings = Arrays.asList( requiredSettings );
    }

    RemoteMembersResolver getHostnameResolver( LogService logService, Config config )
    {
        return this.resolverSupplier.apply( logService, config );
    }

    Collection<Setting<?>> requiredSettings()
    {
        return requiredSettings;
    }
}
