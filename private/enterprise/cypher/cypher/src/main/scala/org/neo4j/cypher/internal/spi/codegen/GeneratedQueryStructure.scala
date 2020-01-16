/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen

import java.lang.reflect.Modifier
import java.util.stream.DoubleStream
import java.util.stream.IntStream
import java.util.stream.LongStream

import org.neo4j.codegen.ClassGenerator
import org.neo4j.codegen.CodeBlock
import org.neo4j.codegen.CodeGeneratorOption
import org.neo4j.codegen.Expression
import org.neo4j.codegen.Expression.constant
import org.neo4j.codegen.Expression.invoke
import org.neo4j.codegen.Expression.newInitializedArray
import org.neo4j.codegen.Expression.newInstance
import org.neo4j.codegen.FieldReference
import org.neo4j.codegen.MethodDeclaration
import org.neo4j.codegen.MethodReference
import org.neo4j.codegen.MethodReference.constructorReference
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen.TypeReference.extending
import org.neo4j.codegen.TypeReference.parameterizedType
import org.neo4j.codegen.TypeReference.typeParameter
import org.neo4j.codegen.api.CodeGeneration.CodeSaver
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.bytecode.ByteCode.VERIFY_GENERATED_BYTECODE
import org.neo4j.codegen.source.SourceCode
import org.neo4j.codegen.source.SourceCode.SOURCECODE
import org.neo4j.codegen.CodeGenerator
import org.neo4j.codegen.Parameter
import org.neo4j.codegen.TypeReference
import org.neo4j.cypher.internal.CodeGenPlanDescriptionHelper
import org.neo4j.cypher.internal.codegen.PrimitiveNodeStream
import org.neo4j.cypher.internal.codegen.PrimitiveRelationshipStream
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.frontend.helpers.using
import org.neo4j.cypher.internal.javacompat.ResultRecord
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ByteCodeMode
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenConfiguration
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.SourceCodeMode
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.AnyValueType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.BoolType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CypherCodeGenType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.FloatType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.ListReferenceType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.LongType
import org.neo4j.cypher.internal.runtime.compiled.codegen.setStaticField
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructureResult
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {

  case class GeneratedQueryStructureResult(query: GeneratedQuery, code: Seq[Argument]) extends CodeStructureResult[GeneratedQuery]

  private def createGenerator(conf: CodeGenConfiguration, code: CodeSaver) = {
    val mode = conf.mode match {
      case SourceCodeMode => SOURCECODE
      case ByteCodeMode => BYTECODE
    }
    val options = mutable.ListBuffer.empty[CodeGeneratorOption]

    if(getClass.desiredAssertionStatus()) {
      options += VERIFY_GENERATED_BYTECODE
    }
    conf.saveSource.foreach(path => {
      options += SourceCode.sourceLocation(path)
    })

    CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode, options ++ code.options:_*)
  }

  override def generateQuery(className: String,
                             columns: Seq[String],
                             operatorIds: Map[String, Id],
                             conf: CodeGenConfiguration)
                            (methodStructure: MethodStructure[_] => Unit)
                            (implicit codeGenContext: CodeGenContext): GeneratedQueryStructureResult = {

    val saver = new CodeSaver(conf.showSource, conf.showByteCode)
    val generator = createGenerator(conf, saver)
    val execution = using(
      generator.generateClass(conf.packageName, className + "Execution", typeRef[GeneratedQueryExecution])) { clazz =>
      val fields: Fields = createFields(columns, clazz)
      setOperatorIds(clazz, operatorIds)
      addSimpleMethods(clazz, fields)
      addAccept(methodStructure, generator, clazz, fields, conf)
      clazz.handle()
    }
    val query = using(generator.generateClass(conf.packageName, className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
                                 param[QueryContext]("queryContext"),
                                 param[QueryProfiler]("tracer"),
                                 param[MapValue]("params"))) { execute =>
        execute.returns(
          invoke(
            newInstance(execution),
            constructorReference(execution,
                                 typeRef[QueryContext],
                                 typeRef[QueryProfiler],
                                 typeRef[MapValue]),
            execute.load("queryContext"),
            execute.load("tracer"),
            execute.load("params")))
      }
      clazz.handle()
    }.newInstance().asInstanceOf[GeneratedQuery]

    val clazz: Class[_] = execution.loadClass()
    operatorIds.foreach {
      case (key, id) =>
        val anyRefId = id.asInstanceOf[AnyRef]
        setStaticField(clazz, key, anyRefId)
    }
    GeneratedQueryStructureResult(query, CodeGenPlanDescriptionHelper.metadata(saver))
  }

  private def addAccept(methodStructure: MethodStructure[_] => Unit,
                        generator: CodeGenerator,
                        clazz: ClassGenerator,
                        fields: Fields,
                        conf: CodeGenConfiguration)(implicit codeGenContext: CodeGenContext) = {
    val exceptionVar = codeGenContext.namer.newVarName()
    using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
      Parameter.param(parameterizedType(classOf[QueryResultVisitor[_]],
        typeParameter("E")), "visitor")).
      parameterizedWith("E", extending(typeRef[Exception])).
      throwsException(typeParameter("E")))) { b: CodeBlock =>
      b.tryCatch(
        (codeBlock: CodeBlock) => {
          val structure = new GeneratedMethodStructure(fields, codeBlock,
                                                       new AuxGenerator(conf.packageName, generator))
          codeBlock.assign(typeRef[ResultRecord], "row",
                           invoke(newInstance(typeRef[ResultRecord]),
                                  MethodReference.constructorReference(typeRef[ResultRecord], typeRef[Int]),
                                  constant(codeGenContext.numberOfColumns()))
                           )
          methodStructure(structure)
          codeBlock.expression(invoke(codeBlock.self(), methodReference(codeBlock.owner(), TypeReference.VOID,
                                                                        "closeCursors")))
        },
        (codeBlock: CodeBlock) => {
          codeBlock.expression(invoke(codeBlock.self(), methodReference(codeBlock.owner(), TypeReference.VOID,
                                                                        "closeCursors")))
          codeBlock.throwException(codeBlock.load(exceptionVar))
        }, param[Throwable](exceptionVar))

    }
  }

  private def addSimpleMethods(clazz: ClassGenerator, fields: Fields) = {
    clazz.generate(Templates.constructor(clazz.handle()))
    Templates.getOrLoadDataRead(clazz, fields)
    Templates.getOrLoadTokenRead(clazz, fields)
    Templates.getOrLoadSchemaRead(clazz, fields)
    Templates.getOrLoadCursors(clazz, fields)
    Templates.nodeCursor(clazz, fields)
    Templates.relationshipScanCursor(clazz, fields)
    Templates.propertyCursor(clazz, fields)
    Templates.closeCursors(clazz, fields)
    clazz.generate(Templates.FIELD_NAMES)
  }

  private def setOperatorIds(clazz: ClassGenerator, operatorIds: Map[String, Id]) = {
    operatorIds.keys.foreach { opId =>
      clazz.publicStaticField(typeRef[Id], opId)
    }
  }

  private def createFields(columns: Seq[String], clazz: ClassGenerator) = {
    clazz.privateStaticFinalField(TypeReference.typeReference(classOf[Array[String]]),
                                  "COLUMNS", newInitializedArray(typeRef[String], columns.map(key => constant(key)):_*))

    Fields(
      entityAccessor = clazz.field(typeRef[TransactionalEntityFactory], "proxySpi"),
      tracer = clazz.field(typeRef[QueryProfiler], "tracer"),
      params = clazz.field(typeRef[MapValue], "params"),
      queryContext = clazz.field(typeRef[QueryContext], "queryContext"),
      cursors = clazz.field(typeRef[CursorFactory], "cursors"),
      nodeCursor = clazz.field(typeRef[NodeCursor], "nodeCursor"),
      relationshipScanCursor = clazz.field(typeRef[RelationshipScanCursor], "relationshipScanCursor"),
      propertyCursor = clazz.field(typeRef[PropertyCursor], "propertyCursor"),
      dataRead =  clazz.field(typeRef[Read], "dataRead"),
      tokenRead =  clazz.field(typeRef[TokenRead], "tokenRead"),
      schemaRead =  clazz.field(typeRef[SchemaRead], "schemaRead"),
      closeables = clazz.field(typeRef[java.util.ArrayList[AutoCloseable]], "closeables")
      )
  }

  def method[O <: AnyRef, R](name: String, params: TypeReference*)
                            (implicit owner: Manifest[O], returns: Manifest[R]): MethodReference =
    MethodReference.methodReference(typeReference(owner), typeReference(returns), name, Modifier.PUBLIC, params: _*)

  def staticField[O <: AnyRef, R](name: String)(implicit owner: Manifest[O], fieldType: Manifest[R]): FieldReference =
    FieldReference.staticField(typeReference(owner), typeReference(fieldType), name)

  def param[T <: AnyRef](name: String)(implicit manifest: Manifest[T]): Parameter =
    Parameter.param(typeReference(manifest), name)

  def typeRef[T](implicit manifest: Manifest[T]): TypeReference = typeReference(manifest)

  def typeReference(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeReference): _*)
    } else {
      base
    }
  }

  def lowerType(cType: CodeGenType): TypeReference = cType match {
    case CypherCodeGenType(symbols.CTNode, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTRelationship, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTInteger, LongType) => typeRef[Long]
    case CypherCodeGenType(symbols.CTFloat, FloatType) => typeRef[Double]
    case CypherCodeGenType(symbols.CTBoolean, BoolType) => typeRef[Boolean]
    case CypherCodeGenType(symbols.ListType(symbols.CTNode), ListReferenceType(LongType)) => typeRef[PrimitiveNodeStream]
    case CypherCodeGenType(symbols.ListType(symbols.CTRelationship), ListReferenceType(LongType)) => typeRef[PrimitiveRelationshipStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) => typeRef[LongStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) => typeRef[DoubleStream]
    case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) => typeRef[IntStream]
    case CodeGenType.javaInt => typeRef[Int]
    case CypherCodeGenType(_, _: AnyValueType) => typeRef[AnyValue]
    case _ => typeRef[Object]
  }

  def lowerTypeScalarSubset(cType: CodeGenType): TypeReference = cType match {
    case CypherCodeGenType(symbols.CTNode, LongType) => lowerType(cType)
    case CypherCodeGenType(symbols.CTRelationship, LongType) => lowerType(cType)
    case CypherCodeGenType(symbols.CTInteger, LongType) => lowerType(cType)
    case CypherCodeGenType(symbols.CTFloat, FloatType) => lowerType(cType)
    case CypherCodeGenType(symbols.CTBoolean, BoolType) => lowerType(cType)
    case CodeGenType.javaInt => lowerType(cType)
    case CypherCodeGenType(_, _: AnyValueType) => lowerType(cType)
    case _ => typeRef[Object]
  }

  def nullValue(cType: CodeGenType): Expression = cType match {
    case CypherCodeGenType(symbols.CTNode, LongType) => constant(-1L)
    case CypherCodeGenType(symbols.CTRelationship, LongType) => constant(-1L)
    case CypherCodeGenType(_, _: AnyValueType) => Templates.noValue
    case _ => constant(null)
  }
}
