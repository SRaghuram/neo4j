package com.neo4j.bench.micro.benchmarks.cypher

object CypherRuntime {
  def from(cypherRuntimeString: String): CypherRuntime =
    cypherRuntimeString match {
      case CompiledByteCode.NAME => CompiledByteCode
      case CompiledSourceCode.NAME => CompiledSourceCode
      case Interpreted.NAME => Interpreted
      case EnterpriseInterpreted.NAME => EnterpriseInterpreted
      case _ => throw new IllegalArgumentException(s"Invalid runtime: $cypherRuntimeString")
    }
}

sealed trait CypherRuntime {
  val debugOptions: Set[String]
}

case object Interpreted extends CypherRuntime {
  final val NAME = "interpreted"
  override val debugOptions: Set[String] = Set()
}

case object EnterpriseInterpreted extends CypherRuntime {
  final val NAME = "slotted"
  override val debugOptions: Set[String] = Set()
}

case object CompiledByteCode extends CypherRuntime {
  final val NAME = "compiled-bytecode"
  override val debugOptions: Set[String] = Set()
}

case object CompiledSourceCode extends CypherRuntime {
  final val NAME = "compiled-sourcecode"
  override val debugOptions: Set[String] = Set("generate_java_source")
}
