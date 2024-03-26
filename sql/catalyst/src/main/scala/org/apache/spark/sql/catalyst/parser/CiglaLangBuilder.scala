/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}

// TODO: Super hacky implementation. Just experimenting with the interfaces...

case class SingleStatement(command: String)
case class MultiStatement(statements: ArrayBuffer[SingleStatement])

trait ProceduralLangInterface {
  def buildInterpreter(batch: String): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(batch: String): ProceduralLangInterpreter = CiglaLangInterpreter(batch)
}

trait CiglaCommand {
  def execute(): Unit
}

case class CiglaSparkStatement(command: String) extends CiglaCommand {
  def execute(): Unit = {
  }
}

trait ProceduralLangInterpreter extends Iterator[CiglaCommand] {
}

case class CiglaLangInterpreter(batch: String) extends ProceduralLangInterpreter {
  // TODO: Keep parser here. We may need for error reporting - e.g. pointing to the
  // exact location of the error.

  // TODO: No need to init this every time..
  private val ciglaParser = new CiglaParser()
  private val parser = ciglaParser.parseBatch(batch)(t => t)

  private val astBuilder = CiglaLangBuilder(batch)
  private val tree = astBuilder.visitMultiStatement(parser.multiStatement())

  private val statements = tree.statements
  private val statementIter = statements.iterator

  override def hasNext: Boolean = statementIter.hasNext

  override def next(): CiglaCommand = {
    val stmt = statementIter.next()
    CiglaSparkStatement(stmt.command)
  }
}

//noinspection ScalaStyle
case class CiglaLangBuilder(batch: String) extends CiglaBaseParserBaseVisitor[AnyRef] {
  override def visitSingleStatement(
      ctx: CiglaBaseParser.SingleStatementContext): SingleStatement = {
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    SingleStatement(command)
  }

  override def visitMultiStatement(ctx: CiglaBaseParser.MultiStatementContext): MultiStatement = {
    val stmts = ctx.singleStatement()
    MultiStatement(stmts.asScala.map(visitSingleStatement)
      .asInstanceOf[ArrayBuffer[SingleStatement]])
  }

  override def visit(parseTree: ParseTree): AnyRef = {
    null
  }
  override def visitChildren(ruleNode: RuleNode): AnyRef = {
    null
  }

  override def visitTerminal(terminalNode: TerminalNode): AnyRef = {
    null
  }

  override def visitErrorNode(errorNode: ErrorNode): AnyRef = {
    null
  }

  override def visitStringLitOrIdentifierOrConstant(
    ctx: CiglaBaseParser.StringLitOrIdentifierOrConstantContext): AnyRef = {
    null
  }
}