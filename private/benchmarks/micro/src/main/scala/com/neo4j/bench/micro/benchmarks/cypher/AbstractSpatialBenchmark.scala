/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.data.CRS
import org.neo4j.graphdb.Label
import org.neo4j.values.storable.CoordinateReferenceSystem

abstract class AbstractSpatialBenchmark extends AbstractCypherBenchmark {
  private val INDEX_EXTENT_CARTESIAN_X: Int = 2000000
  private val INDEX_EXTENT_CARTESIAN_Y: Int = 2000000
  private val INDEX_EXTENT_WGS84_X: Int = 360
  private val INDEX_EXTENT_WGS84_Y: Int = 180
  val NODE_COUNT: Int = 10000000
  val LABEL: Label = Label.label("SampleLabel")
  val KEY: String = "key"

  // set no smaller than smallest positive number, as 0 would disable the threshold optimization completely
  def calculateBottomThreshold(thresholdTop: Double, thresholdDelta: Double): Double =
    Math.max(thresholdTop - thresholdDelta, java.lang.Double.MIN_VALUE)

  // TODO review
  def computeQueryDistance(crs: CoordinateReferenceSystem,
                           dataExtentX: Double,
                           dataExtentY: Double,
                           ratio: Double): Double = crs match {
    case CoordinateReferenceSystem.Cartesian =>
      Math.min(dataExtentX, dataExtentY) * ratio / 2
    case CoordinateReferenceSystem.WGS84 =>
      /*
      // TODO notes
      10,000,000 meters ~= distance from equator to a pole
      40,000,000 meters ~= circumference of earth at equator
       */
      10000000 * ratio // TODO wtf?
  }

  def crsSetting: CRS

  /*
  Specifies how data extents will be scaled relative to default data extents
   */
  def dataExtentsRatio: Double

  /*
  Specifies how query extents will be scaled relative to already scaled (after applying dataExtentsRatio) data extents
   */
  def queryExtentsRatio: Double

  private def defaultDataExtentX: Double = crsSetting.crs() match {
    case CoordinateReferenceSystem.Cartesian => INDEX_EXTENT_CARTESIAN_X
    case CoordinateReferenceSystem.WGS84 => INDEX_EXTENT_WGS84_X
  }

  private def defaultDataExtentY: Double = crsSetting.crs() match {
    case CoordinateReferenceSystem.Cartesian => INDEX_EXTENT_CARTESIAN_Y
    case CoordinateReferenceSystem.WGS84 => INDEX_EXTENT_WGS84_Y
  }

  def dataExtentX: Double = defaultDataExtentX * dataExtentsRatio

  def dataExtentY: Double = defaultDataExtentY * dataExtentsRatio

  def dataExtentMinX: Double = -dataExtentX / 2

  def dataExtentMaxX: Double = dataExtentX / 2

  def dataExtentMinY: Double = -dataExtentY / 2

  def dataExtentMaxY: Double = dataExtentY / 2

  def searchExtentX: Double = dataExtentX * queryExtentsRatio

  def searchExtentY: Double = dataExtentY * queryExtentsRatio

  def searchExtentMinX: Double = dataExtentMinX + (searchExtentX / 2)

  def searchExtentMaxX: Double = dataExtentMaxX - (searchExtentX / 2)

  def searchExtentMinY: Double = dataExtentMinY + (searchExtentY / 2)

  def searchExtentMaxY: Double = dataExtentMaxY - (searchExtentY / 2)
}


// Specifies how points are to be distributed within the 2D space, during data generation
object DataDistribution {
  def from(distributionString: String): DataDistribution =
    distributionString match {
      case RandomDistribution.NAME => RandomDistribution
      case GridDistribution.NAME => GridDistribution
      case ClusteredDistribution.NAME => ClusteredDistribution
      case _ => throw new IllegalArgumentException(s"Invalid data distribution: $distributionString")
    }
}

sealed trait DataDistribution

// uniform random selection of x & y coordinates
case object RandomDistribution extends DataDistribution {
  final val NAME = "random"
}

// uniform deterministic selection of x & y coordinates, in grid pattern
// grid resolution is function of node count & the point placement strategy
case object GridDistribution extends DataDistribution {
  final val NAME = "grid"
}

// clusters of points, of varying density, within extent defined by point placement strategy
case object ClusteredDistribution extends DataDistribution {
  final val NAME = "clusters"
}

// Specifies at which coordinates points will be placed during data generation
object DataExtent {
  def from(pointPlacementString: String): DataExtent =
    pointPlacementString match {
      case IndexExtent.NAME => IndexExtent
      case QueryExtent.NAME => QueryExtent
      case GlobalExtent.NAME => GlobalExtent
      case _ => throw new IllegalArgumentException(s"Invalid point placement: $pointPlacementString")
    }
}

sealed trait DataExtent

// within index extent
case object IndexExtent extends DataExtent {
  final val NAME = "index"
}

// within query extent
case object QueryExtent extends DataExtent {
  final val NAME = "query"
}

// both within and outside of index extent
case object GlobalExtent extends DataExtent {
  final val NAME = "global"
}


// specifies location of query search space, within search area (not index extent only)
object QueryLocation {
  def from(locationString: String): QueryLocation =
    locationString match {
      case CenterLocation.NAME => CenterLocation
      case BorderLocation.NAME => BorderLocation
      case CornerLocation.NAME => CornerLocation
      case OutsideLocation.NAME => OutsideLocation
      case _ => throw new IllegalArgumentException(s"Invalid query location: $locationString")
    }
}

sealed trait QueryLocation

case object CenterLocation extends QueryLocation {
  final val NAME = "center"
}

case object BorderLocation extends QueryLocation {
  final val NAME = "edge"
}

case object CornerLocation extends QueryLocation {
  final val NAME = "corner"
}

case object OutsideLocation extends QueryLocation {
  final val NAME = "outside"
}
