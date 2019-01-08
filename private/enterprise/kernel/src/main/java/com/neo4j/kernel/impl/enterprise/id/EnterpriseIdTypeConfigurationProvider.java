/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.id;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;

/**
 * Id type configuration provider for enterprise edition.
 * Allow to reuse predefined id types that are reused in community and in
 * addition to that allow additional id types to reuse be specified by
 * {@link CommercialEditionSettings#idTypesToReuse} setting.
 *
 * @see IdType
 * @see IdTypeConfiguration
 */
public class EnterpriseIdTypeConfigurationProvider extends CommunityIdTypeConfigurationProvider
{
    private final Set<IdType> typesToReuse;

    public EnterpriseIdTypeConfigurationProvider( Config config )
    {
        typesToReuse = configureReusableTypes( config );
    }

    @Override
    protected Set<IdType> getTypesToReuse()
    {
        return typesToReuse;
    }

    private EnumSet<IdType> configureReusableTypes( Config config )
    {
        EnumSet<IdType> types = EnumSet.copyOf( super.getTypesToReuse() );
        List<IdType> configuredTypes = config.get( CommercialEditionSettings.idTypesToReuse );
        types.addAll( configuredTypes );
        return types;
    }
}
