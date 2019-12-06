/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.plans

import java.lang.reflect.Method

import org.neo4j.cypher.internal.ir.{SinglePlannerQuery, Strictness}
import org.neo4j.cypher.internal.expressions._
import org.neo4j.cypher.internal.util.Foldable._
import org.neo4j.cypher.internal.util.Rewritable._
import org.neo4j.cypher.internal.util.attribution.{Id, IdGen, Identifiable, SameId}
import org.neo4j.cypher.internal.util.{Foldable, Rewritable}
import org.neo4j.exceptions.InternalException

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object LogicalPlan {
  val LOWEST_TX_LAYER = 0
}

/*
A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
application of the corresponding operator on the relations generated by its children, the result of which is then sent
further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
to data in the database, to the root, which is the final operator producing the query answer. */
abstract class LogicalPlan(idGen: IdGen)
  extends Product
  with Foldable
  with Strictness
  with Rewritable
  with Identifiable {

  self =>

  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]
  def availableSymbols: Set[String]

  override val id: Id = idGen.id()

  override val hashCode: Int = MurmurHash3.productHash(self)

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[LogicalPlan]) false
    else {
      val otherPlan = obj.asInstanceOf[LogicalPlan]
      if (this.eq(otherPlan)) return true
      if (this.getClass != otherPlan.getClass) return false
      val stack = new mutable.Stack[(Iterator[Any], Iterator[Any])]()
      var p1 = this.productIterator
      var p2 = otherPlan.productIterator
      while (p1.hasNext && p2.hasNext) {
        val continue =
          (p1.next, p2.next) match {
            case (lp1:LogicalPlan, lp2:LogicalPlan) =>
              if (lp1.getClass != lp2.getClass) {
                false
              } else {
                stack.push((p1, p2))
                p1 = lp1.productIterator
                p2 = lp2.productIterator
                true
              }
            case (_:LogicalPlan, _) => false
            case (_, _:LogicalPlan) => false
            case (a1, a2) => a1 == a2
          }

        if (!continue) return false
        while (!p1.hasNext && !p2.hasNext && stack.nonEmpty) {
          val (p1New, p2New) = stack.pop
          p1 = p1New
          p2 = p2New
        }
      }
      p1.isEmpty && p2.isEmpty
    }
  }

  def leaves: Seq[LogicalPlan] = this.treeFold(Seq.empty[LogicalPlan]) {
    case plan: LogicalPlan
      if plan.lhs.isEmpty && plan.rhs.isEmpty => acc => (acc :+ plan, Some(identity))
  }

  def copyPlanWithIdGen(idGen: IdGen): LogicalPlan = {
    try {
      val arguments = this.children.toList :+ idGen
      copyConstructor.invoke(this, arguments: _*).asInstanceOf[this.type]
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("wrong number of arguments") =>
        throw new InternalException("Logical plans need to be case classes, and have the IdGen in a separate constructor", e)
    }
  }

  lazy val copyConstructor: Method = this.getClass.getMethods.find(_.getName == "copy").get

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toIndexedSeq
      val resultingPlan =
        if (params.length == args.length + 1
          && params.last.isAssignableFrom(classOf[IdGen]))
          constructor.invoke(this, args :+ SameId(this.id): _*).asInstanceOf[this.type]
        else if ((params.length == args.length + 2)
          && params(params.length - 2).isAssignableFrom(classOf[SinglePlannerQuery])
          && params(params.length - 1).isAssignableFrom(classOf[IdGen]))
          constructor.invoke(this, args :+ SameId(this.id): _*).asInstanceOf[this.type]
        else
          constructor.invoke(this, args: _*).asInstanceOf[this.type]
      resultingPlan
    }

  def isLeaf: Boolean = lhs.isEmpty && rhs.isEmpty

  override def toString: String = {
    def indent(level: Int, in: String): String = level match {
      case 0 => in
      case _ => System.lineSeparator() + "  " * level + in
    }

    val childrenHeap = new scala.collection.mutable.Stack[(String, Int, Option[LogicalPlan])]
    childrenHeap.push(("", 0, Some(this)))
    val sb = new StringBuilder()

    while (childrenHeap.nonEmpty) {
      childrenHeap.pop() match {
        case (prefix, level, Some(plan)) =>
          val children = plan.lhs.toIndexedSeq ++ plan.rhs.toIndexedSeq
          val nonChildFields = plan.productIterator.filterNot(children.contains).mkString(", ")
          val prodPrefix = plan.productPrefix
          sb.append(indent(level, s"""$prefix$prodPrefix($nonChildFields) {""".stripMargin))

          (plan.lhs, plan.rhs) match {
            case (None, None) =>
              sb.append("}")
            case (Some(_), None) =>
              childrenHeap.push((System.lineSeparator() + "  " * level + "}", level + 1, None))
              childrenHeap.push(("LHS -> ", level + 1, plan.lhs))
            case _ =>
              childrenHeap.push((System.lineSeparator() + "  " * level + "}", level + 1, None))
              childrenHeap.push(("RHS -> ", level + 1, plan.rhs))
              childrenHeap.push(("LHS -> ", level + 1, plan.lhs))
          }
        case (prefix, _, _) =>
          sb.append(prefix)
      }
    }

    sb.toString()
  }

  def satisfiesExpressionDependencies(e: Expression): Boolean = e.dependencies.map(_.name).forall(availableSymbols.contains)

  def debugId: String = f"0x$hashCode%08x"

  def flatten: Seq[LogicalPlan] = Flattener.create(this)

  def indexUsage: Seq[IndexUsage] = {
    import org.neo4j.cypher.internal.util.Foldable._
    this.fold(Seq.empty[IndexUsage]) {
      case NodeIndexSeek(idName, label, properties, _, _, _) =>
        acc => acc :+ SchemaIndexSeekUsage(idName, label.nameId.id, label.name, properties.map(_.propertyKeyToken.name))
      case NodeUniqueIndexSeek(idName, label, properties, _, _, _) =>
        acc => acc :+ SchemaIndexSeekUsage(idName, label.nameId.id, label.name, properties.map(_.propertyKeyToken.name))
      case NodeIndexScan(idName, label, properties, _, _) =>
        acc => acc :+ SchemaIndexScanUsage(idName, label.nameId.id, label.name, properties.map(_.propertyKeyToken.name))
      }
  }
}

// Marker interface for all plans that aggregate inputs.
trait AggregatingPlan extends LogicalPlan

// Marker interface for all plans that performs updates
trait UpdatingPlan extends LogicalPlan

abstract class LogicalLeafPlan(idGen: IdGen) extends LogicalPlan(idGen) with LazyLogicalPlan {
  final val lhs = None
  final val rhs = None
  def argumentIds: Set[String]
}

abstract class NodeLogicalLeafPlan(idGen: IdGen) extends LogicalLeafPlan(idGen) {
  def idName: String
}

abstract class IndexLeafPlan(idGen: IdGen) extends NodeLogicalLeafPlan(idGen) {
  /**
    * Indexed properties that will be retrieved from the index and cached in the row.
    */
  def cachedProperties: Seq[CachedProperty] = properties.flatMap(_.maybeCachedProperty(idName))

  /**
    * All properties
    */
  def properties: Seq[IndexedProperty]

  /**
    * Create a copy of this plan, swapping out the properties
    * @return
    */
  def withProperties(properties: Seq[IndexedProperty]): IndexLeafPlan

  /**
    * Get a copy of this index plan where getting values is disabled
    */
  def copyWithoutGettingValues: IndexLeafPlan
}

abstract class IndexSeekLeafPlan(idGen: IdGen) extends IndexLeafPlan(idGen) {

  def valueExpr: QueryExpression[Expression]
}

case object Flattener extends LogicalPlans.Mapper[Seq[LogicalPlan]] {
  override def onLeaf(plan: LogicalPlan): Seq[LogicalPlan] = Seq(plan)

  override def onOneChildPlan(plan: LogicalPlan, source: Seq[LogicalPlan]): Seq[LogicalPlan] = plan +: source

  override def onTwoChildPlan(plan: LogicalPlan, lhs: Seq[LogicalPlan], rhs: Seq[LogicalPlan]): Seq[LogicalPlan] = (plan +: lhs) ++ rhs

  def create(plan: LogicalPlan): Seq[LogicalPlan] =
    LogicalPlans.map(plan, this)
}

sealed trait IndexUsage {
  def identifier:String
}

final case class SchemaIndexSeekUsage(identifier: String, labelId : Int, label: String, propertyKeys: Seq[String]) extends IndexUsage
final case class SchemaIndexScanUsage(identifier: String, labelId : Int, label: String, propertyKeys: Seq[String]) extends IndexUsage
