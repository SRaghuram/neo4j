/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.id.IdType;

import static org.neo4j.configuration.Settings.LONG;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.list;
import static org.neo4j.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.configuration.Settings.optionsObeyCase;
import static org.neo4j.configuration.Settings.setting;

/**
 * Enterprise edition specific settings
 */
@ServiceProvider
public class CommercialEditionSettings implements LoadableConfig
{
    public static final String COMMERCIAL_SECURITY_MODULE_ID = "commercial-security-module";

    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'node' and 'relationship' types are supported. " )
    public static final Setting<List<IdType>> idTypesToReuse = setting(
            "dbms.ids.reuse.types.override", list( ",", optionsIgnoreCase( IdType.NODE, IdType.RELATIONSHIP ) ),
            String.join( ",", IdType.RELATIONSHIP.name(), IdType.NODE.name() ) );

    @Description( "The maximum number of databases." )
    public static final Setting<Long> maxNumberOfDatabases = setting( "dbms.max_databases", LONG, "100" );

    @Internal
    public static final Setting<String> security_module = setting( "unsupported.dbms.security.module", STRING, COMMERCIAL_SECURITY_MODULE_ID );

    @Description( "Configure the operating mode of the database -- 'SINGLE' for stand-alone operation, " +
            "'CORE' for operating as a core member of a Causal Cluster, " +
            "or 'READ_REPLICA' for operating as a read replica member of a Causal Cluster." )
    public static final Setting<Mode> mode = setting( "dbms.mode", optionsObeyCase( Mode.class ), Mode.SINGLE.name() );

    public enum Mode
    {
        SINGLE,
        CORE,
        READ_REPLICA
    }
}
