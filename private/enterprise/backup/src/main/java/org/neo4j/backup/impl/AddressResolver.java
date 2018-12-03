/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

class AddressResolver
{
    AdvertisedSocketAddress resolveCorrectAddress( Config config, OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress defaultValue = readDefaultConfigAddress( config );
        return new AdvertisedSocketAddress( userProvidedAddress.getHostname().orElse( defaultValue.getHostname() ),
                userProvidedAddress.getPort().orElse( defaultValue.getPort() ) );
    }

    private AdvertisedSocketAddress readDefaultConfigAddress( Config config )
    {
        return asAdvertised( config.get( OnlineBackupSettings.online_backup_listen_address ) );
    }

    private AdvertisedSocketAddress asAdvertised( ListenSocketAddress listenSocketAddress )
    {
        return new AdvertisedSocketAddress( listenSocketAddress.getHostname(), listenSocketAddress.getPort() );
    }
}
