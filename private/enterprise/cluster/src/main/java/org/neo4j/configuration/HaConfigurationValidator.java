/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;
import org.neo4j.logging.Log;

import static org.neo4j.cluster.ClusterSettings.initial_hosts;
import static org.neo4j.cluster.ClusterSettings.server_id;

public class HaConfigurationValidator implements ConfigurationValidator
{
    @Override
    public Map<String,String> validate( @Nonnull Config config, @Nonnull Log log ) throws InvalidSettingException
    {
        // Make sure mode is HA
        Mode mode = config.get( EnterpriseEditionSettings.mode );
        if ( mode.equals( Mode.HA ) || mode.equals( Mode.ARBITER ) )
        {
            validateServerId( config );
            validateInitialHosts( config );
        }

        return Collections.emptyMap();
    }

    private static void validateServerId( Config config )
    {
        if ( !config.isConfigured( server_id ) )
        {
            throw new InvalidSettingException( String.format( "Missing mandatory value for '%s'", server_id.name() ) );
        }
    }

    private static void validateInitialHosts( Config config )
    {
        List<HostnamePort> hosts = config.get( initial_hosts );
        if ( hosts == null || hosts.isEmpty() )
        {
            throw new InvalidSettingException(
                    String.format( "Missing mandatory non-empty value for '%s'", initial_hosts.name() ) );
        }
    }
}
