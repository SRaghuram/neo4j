/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3

import scala.language.implicitConversions

package object symbols {
  val CTAny: AnyType = AnyType.instance
  val CTBoolean: BooleanType = BooleanType.instance
  val CTString: StringType = StringType.instance
  val CTNumber: NumberType = NumberType.instance
  val CTFloat: FloatType = FloatType.instance
  val CTInteger: IntegerType = IntegerType.instance
  val CTMap: MapType = MapType.instance
  val CTNode: NodeType = NodeType.instance
  val CTRelationship: RelationshipType = RelationshipType.instance
  val CTPoint: PointType = PointType.instance
  val CTGeometry: GeometryType = GeometryType.instance
  val CTPath: PathType = PathType.instance
  val CTGraphRef: GraphRefType = GraphRefType.instance
  def CTList(inner: CypherType): ListType = ListType(inner)

  implicit def invariantTypeSpec(that: CypherType): TypeSpec = that.invariant
}
