/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

/**
 * Enterprise edition specific settings
 */
@ServiceProvider
public class CommercialEditionSettings implements SettingsDeclaration
{
    public static final String COMMERCIAL_SECURITY_MODULE_ID = "commercial-security-module";

    @Description( "The maximum number of databases." )
    public static final Setting<Long> maxNumberOfDatabases = newBuilder( "dbms.max_databases", LONG, 100L ).addConstraint( min( 2L ) ).build();

    @Internal
    public static final Setting<String> security_module =
            newBuilder( "unsupported.dbms.security.module", SettingValueParsers.STRING, COMMERCIAL_SECURITY_MODULE_ID ).build();

    @Description( "Configure the operating mode of the database -- 'SINGLE' for stand-alone operation, " +
            "'CORE' for operating as a core member of a Causal Cluster, " + "or 'READ_REPLICA' for operating as a read replica member of a Causal Cluster." )
    public static final Setting<Mode> mode = newBuilder( "dbms.mode", ofEnum( Mode.class ), Mode.SINGLE ).build();

    public enum Mode
    {
        SINGLE,
        CORE,
        READ_REPLICA
    }
}
