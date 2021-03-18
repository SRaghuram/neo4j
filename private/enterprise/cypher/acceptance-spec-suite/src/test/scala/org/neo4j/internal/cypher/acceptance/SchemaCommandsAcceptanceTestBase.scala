/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30

abstract class SchemaCommandsAcceptanceTestBase extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {
  val nativeProvider: String = GenericNativeIndexProvider.DESCRIPTOR.name()
  val nativeLuceneProvider: String = NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name()
  val fulltextProvider: String = FulltextIndexProviderFactory.DESCRIPTOR.name()
  val cartesianMin: String = SPATIAL_CARTESIAN_MIN.getSettingName
  val cartesianMax: String = SPATIAL_CARTESIAN_MAX.getSettingName
  val cartesian3dMin: String = SPATIAL_CARTESIAN_3D_MIN.getSettingName
  val cartesian3dMax: String = SPATIAL_CARTESIAN_3D_MAX.getSettingName
  val wgsMin: String = SPATIAL_WGS84_MIN.getSettingName
  val wgsMax: String = SPATIAL_WGS84_MAX.getSettingName
  val wgs3dMin: String = SPATIAL_WGS84_3D_MIN.getSettingName
  val wgs3dMax: String = SPATIAL_WGS84_3D_MAX.getSettingName
  val eventuallyConsistent: String = FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName
  val analyzer: String = FULLTEXT_ANALYZER.getSettingName
}
