/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@RunWith( Parameterized.class )
public class HaConfigurationValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameterized.Parameter
    public EnterpriseEditionSettings.Mode mode;

    @Parameterized.Parameters( name = "{0}" )
    public static List<EnterpriseEditionSettings.Mode> recordFormats()
    {
        return Arrays.asList( EnterpriseEditionSettings.Mode.HA, EnterpriseEditionSettings.Mode.ARBITER );
    }

    @Test
    public void validateOnlyIfModeIsHA()
    {
        // when
        Config config = Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), EnterpriseEditionSettings.Mode.SINGLE.name(),
                        ClusterSettings.initial_hosts.name(), "" ) )
                .withValidator( new HaConfigurationValidator() ).build();

        // then
        Optional<String> value = config.getRaw( ClusterSettings.initial_hosts.name() );
        assertTrue( value.isPresent() );
        assertEquals( "", value.get() );
    }

    @Test
    public void validateSuccess()
    {
        // when
        Config config = Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), mode.name(),
                        ClusterSettings.server_id.name(), "1",
                        ClusterSettings.initial_hosts.name(), "localhost,remotehost" ) )
                .withValidator( new HaConfigurationValidator() ).build();

        // then
        assertEquals( asList( new HostnamePort( "localhost" ),
                new HostnamePort( "remotehost" ) ),
                config.get( ClusterSettings.initial_hosts ) );
        assertEquals( new InstanceId( 1 ), config.get( ClusterSettings.server_id ) );
    }

    @Test
    public void missingServerId()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory value for 'ha.server_id'" );

        // when
        Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), mode.name() ) )
                .withValidator( new HaConfigurationValidator() ).build();
    }

    @Test
    public void missingInitialHosts()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory non-empty value for 'ha.initial_hosts'" );

        // when
        Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), mode.name(),
                        ClusterSettings.server_id.name(), "1" ) )
                .withValidator( new HaConfigurationValidator() ).build();
    }

    @Test
    public void initialHostsEmpty()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory non-empty value for 'ha.initial_hosts'" );

        // when
        Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), mode.name(),
                        ClusterSettings.server_id.name(), "1",
                        ClusterSettings.initial_hosts.name(), "," ) )
                .withValidator( new HaConfigurationValidator() ).build();
    }
}
