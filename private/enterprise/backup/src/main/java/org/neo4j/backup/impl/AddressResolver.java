/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

class AddressResolver
{
    HostnamePort resolveCorrectHAAddress( Config config, OptionalHostnamePort userProvidedAddress )
    {
        HostnamePort defaultValues = readDefaultConfigAddressHA( config );
        return new HostnamePort( userProvidedAddress.getHostname().orElse( defaultValues.getHost() ),
                userProvidedAddress.getPort().orElse( defaultValues.getPort() ) );
    }

    AdvertisedSocketAddress resolveCorrectCCAddress( Config config, OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress defaultValue = readDefaultConfigAddressCC( config );
        return new AdvertisedSocketAddress( userProvidedAddress.getHostname().orElse( defaultValue.getHostname() ),
                userProvidedAddress.getPort().orElse( defaultValue.getPort() ) );
    }

    private HostnamePort readDefaultConfigAddressHA( Config config )
    {
        return config.get( OnlineBackupSettings.online_backup_server );
    }

    private AdvertisedSocketAddress readDefaultConfigAddressCC( Config config )
    {
        return asAdvertised( config.get( OnlineBackupSettings.online_backup_server ) );
    }

    private AdvertisedSocketAddress asAdvertised( HostnamePort listenSocketAddress )
    {
        return new AdvertisedSocketAddress( listenSocketAddress.getHost(), listenSocketAddress.getPort() );
    }
}
