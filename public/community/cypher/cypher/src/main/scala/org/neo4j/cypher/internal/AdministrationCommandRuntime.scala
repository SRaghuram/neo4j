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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.string.UTF8
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

trait AdministrationCommandRuntime extends CypherRuntime[RuntimeContext] {
  protected val followerError = "Administration commands must be executed on the LEADER server."
  protected val secureHasher = new SecureHasher

  def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean

  def validatePassword(password: Array[Byte]): Array[Byte] = {
    if (password == null || password.length == 0) throw new InvalidArgumentsException("A password cannot be empty.")
    password
  }

  def hashPassword(initialPassword: Array[Byte]): TextValue = {
    try {
      Values.utf8Value(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize())
    } finally {
      //TODO: Make this work again (we have places we need this un-zero'd later for checking if password is duplicated)
      //if (initialPassword != null) java.util.Arrays.fill(initialPassword, 0.toByte)
    }
  }

  def getPasswordFieldsCurrent(password: Either[Array[Byte], AnyRef]): (String, Value, MapValueConverter) =
    getPasswordFields(password, prefix = "__current_internal_", hashPw = false)

  def getPasswordFields(password: Either[Array[Byte], AnyRef],
                        prefix: String = "__internal_",
                        rename: String => String = s => s,
                        hashPw: Boolean = true): (String, Value, MapValueConverter) = password match {
    case Left(encodedPassword) =>
      validatePassword(encodedPassword)
      if (hashPw) {
        (rename(s"${prefix}credentials"), hashPassword(encodedPassword), IdentityConverter)
      } else {
        (rename(s"${prefix}credentials"), Values.byteArray(encodedPassword), IdentityConverter)
      }
    case Right(pw) if pw.isInstanceOf[ParameterFromSlot] =>
      // JVM type erasure means at runtime we get a type that is not actually expected by the Scala compiler, so we cannot use case Right(parameterPassword)
      val parameterPassword = pw.asInstanceOf[ParameterFromSlot]
      validatePasswordParameterType(parameterPassword)
      (rename(parameterPassword.name), Values.NO_VALUE, PasswordParameterConverter(parameterPassword.name, rename, hashPw = hashPw))
  }

  private def getValidPasswordParameter(params: MapValue, passwordParameter: String): String = {
    params.get(passwordParameter) match {
      case s: StringValue =>
        s.stringValue()
      case Values.NO_VALUE =>
        throw new ParameterNotFoundException(s"Expected parameter(s): $passwordParameter")
      case other =>
        throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + other.getTypeName)
    }
  }

  private def validatePasswordParameterType(param: ParameterFromSlot): Unit = {
    param.parameterType match {
      case _:StringType =>
      case _ => throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + param.parameterType)
    }
  }

  trait MapValueConverter extends Function[MapValue, MapValue] {
    def overlaps(other: MapValueConverter) = false

    def apply(params: MapValue): MapValue = params
  }

  case object IdentityConverter extends MapValueConverter

  case class PasswordParameterConverter(passwordParameter: String, rename: String => String = s => s, hashPw: Boolean = true) extends MapValueConverter {
    override def overlaps(other: MapValueConverter): Boolean = other match {
      case PasswordParameterConverter(name, _, _) => name == passwordParameter
      case _ => false
    }

    override def apply(params: MapValue): MapValue = {
      val encodedPassword = UTF8.encode(getValidPasswordParameter(params, passwordParameter))
      validatePassword(encodedPassword)
      if (hashPw) {
        val hashedPassword = hashPassword(encodedPassword)
        params.updatedWith(rename(passwordParameter), hashedPassword)
      } else {
        params.updatedWith(rename(passwordParameter), Values.byteArray(encodedPassword))
      }
    }
  }

  def makeCreateUserExecutionPlan(userName: String,
                                  password: Either[Array[Byte], Parameter],
                                  requirePasswordChange: Boolean,
                                  suspended: Boolean)(
                                   sourcePlan: Option[ExecutionPlan],
                                   normalExecutionEngine: ExecutionEngine): ExecutionPlan = {
    val (credentialsKey, credentialsValue, credentialsConverter) = getPasswordFields(password)
    val mapValueConverter: MapValue => MapValue = credentialsConverter
    UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
      // NOTE: If username already exists we will violate a constraint
      s"""CREATE (u:User {name: $$name, credentials: $$$credentialsKey, passwordChangeRequired: $$passwordChangeRequired, suspended: $$suspended})
         |RETURN u.name""".stripMargin,
      VirtualValues.map(
        Array("name", credentialsKey, "passwordChangeRequired", "suspended"),
        Array(
          Values.utf8Value(userName),
          credentialsValue,
          Values.booleanValue(requirePasswordChange),
          Values.booleanValue(suspended))),
      QueryHandler
        .handleNoResult(() => Some(new IllegalStateException(s"Failed to create the specified user '$userName'.")))
        .handleError(error => (error, error.getCause) match {
          case (_, _: UniquePropertyValueValidationException) =>
            new InvalidArgumentsException(s"Failed to create the specified user '$userName': User already exists.", error)
          case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
            new DatabaseAdministrationOnFollowerException(s"Failed to create the specified user '$userName': $followerError", error)
          case _ => new IllegalStateException(s"Failed to create the specified user '$userName'.", error)
        }),
      sourcePlan,
      parameterConverter = mapValueConverter
    )
  }

}
