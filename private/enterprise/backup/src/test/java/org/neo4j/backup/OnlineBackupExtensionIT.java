/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import java.util.Map;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactoryContractTest;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;

public class OnlineBackupExtensionIT extends KernelExtensionFactoryContractTest
{
    public OnlineBackupExtensionIT()
    {
        super( OnlineBackupExtensionFactory.KEY, OnlineBackupExtensionFactory.class );
    }

    @Override
    protected Map<String, String> configuration( int instance )
    {
        Map<String, String> configuration = super.configuration( instance );
        configuration.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.TRUE );
        configuration.put( OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort() );
        return configuration;
    }
}
