/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.neo4j.kernel.impl.enterprise.lock.forseti.DeadlockStrategies;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@ServiceProvider
public class EnterpriseInternalSettings implements SettingsDeclaration
{
    @Internal
    public static final Setting<DeadlockStrategies> forseti_deadlock_resolution_strategy =
            newBuilder( "unsupported.dbms.locks.forseti_deadlock_resolution_strategy", ofEnum( DeadlockStrategies.class ),
                    DeadlockStrategies.ABORT_YOUNG ).build();
}
