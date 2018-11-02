/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.configuration;

import java.util.List;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.kernel.configuration.Settings.optionsObeyCase;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;

/**
 * Enterprise edition specific settings
 */
public class EnterpriseEditionSettings implements LoadableConfig
{
    public static final String ENTERPRISE_SECURITY_MODULE_ID = "enterprise-security-module";

    @Description( "Specified names of id types (comma separated) that should be reused. " +
                  "Currently only 'node' and 'relationship' types are supported. " )
    public static final Setting<List<IdType>> idTypesToReuse = setting(
            "dbms.ids.reuse.types.override", list( ",", optionsIgnoreCase( NODE, RELATIONSHIP ) ),
            String.join( ",", IdType.RELATIONSHIP.name(), IdType.NODE.name() ) );

    @Internal
    public static final Setting<String> security_module = setting( "unsupported.dbms.security.module", STRING,
            ENTERPRISE_SECURITY_MODULE_ID );

    @Description( "Configure the operating mode of the database -- 'SINGLE' for stand-alone operation, " +
            "'HA' for operating as a member in an HA cluster, 'ARBITER' for a cluster member with no database in an HA cluster, " +
            "'CORE' for operating as a core member of a Causal Cluster, " +
            "or 'READ_REPLICA' for operating as a read replica member of a Causal Cluster." )
    public static final Setting<Mode> mode = setting( "dbms.mode", optionsObeyCase( Mode.class ), Mode.SINGLE.name() );

    public enum Mode
    {
        SINGLE,
        HA,
        ARBITER,
        CORE,
        READ_REPLICA
    }
}
