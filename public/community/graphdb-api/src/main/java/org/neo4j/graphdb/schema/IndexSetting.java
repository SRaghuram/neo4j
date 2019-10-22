/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.schema;

import org.neo4j.annotations.api.PublicApi;

/**
 * Index settings are used for fine-tuning the behaviour of schema indexes.
 * All indexes schema indexes have a configuration associated with them, and it is only necessary to specify the particular settings in that configuration,
 * that should differ from their defaults.
 * <p>
 * Index settings can only be specified when the index is created.
 * Once the index has been created, the index configuration becomes immutable.
 * <p>
 * Here is an example where a full-text index is created with a custom analyzer:
 *
 * <pre><code>
 *     try ( Transaction tx = database.beginTx() )
 *     {
 *         tx.schema().indexFor( Label.label( "Email" ) ).on( "from" ).on( "to" ).on( "cc" ).on( "bcc" )
 *                 .withName( "email-addresses" )
 *                 .withIndexType( IndexType.FULLTEXT )
 *                 .withIndexConfiguration( Map.of( IndexSetting.FULLTEXT_ANALYZER, "email" ) )
 *                 .create();
 *         tx.commit();
 *     }
 * </code></pre>
 */
@PublicApi
public enum IndexSetting
{
    /**
     * Configure the analyzer used in a full-text index, indexes of type {@link IndexType#FULLTEXT}.
     * <p>
     * The list of possible analyzers are available via the {@code db.index.fulltext.listAvailableAnalyzers()} procedure.
     */
    FULLTEXT_ANALYZER( "fulltext.analyzer", String.class ),
    /**
     * Configure if a full-text index is allowed to be eventually consistent.
     * By default full-text indexes are fully consistent, just like other schema indexes.
     */
    FULLTEXT_EVENTUALLY_CONSISTENT( "fulltext.eventually_consistent", Boolean.class ),
    SPATIAL_CARTESIAN_MAX_LEVELS( "spatial.cartesian.maxLevels", Integer.class ),
    SPATIAL_CARTESIAN_MIN( "spatial.cartesian.min", double[].class ),
    SPATIAL_CARTESIAN_MAX( "spatial.cartesian.max", double[].class ),
    SPATIAL_CARTESIAN_3D_MAX_LEVELS( "spatial.cartesian-3d.maxLevels", Integer.class ),
    SPATIAL_CARTESIAN_3D_MIN( "spatial.cartesian-3d.min", double[].class ),
    SPATIAL_CARTESIAN_3D_MAX( "spatial.cartesian-3d.max", double[].class ),
    SPATIAL_WGS84_MAX_LEVELS( "spatial.wgs-84.maxLevels", Integer.class ),
    SPATIAL_WGS84_MIN( "spatial.wgs-84.min", double[].class ),
    SPATIAL_WGS84_MAX( "spatial.wgs-84.max", double[].class ),
    SPATIAL_WGS84_3D_MAX_LEVELS( "spatial.wgs-84-3d.maxLevels", Integer.class ),
    SPATIAL_WGS84_3D_MIN( "spatial.wgs-84-3d.min", double[].class ),
    SPATIAL_WGS84_3D_MAX( "spatial.wgs-84-3d.max", double[].class ),
    ;

    private final String settingName;
    private final Class<?> valueType;

    IndexSetting( String settingName, Class<?> valueType )
    {

        this.settingName = settingName;
        this.valueType = valueType;
    }

    public String getSettingName()
    {
        return settingName;
    }

    public Class<?> getType()
    {
        return valueType;
    }
}
