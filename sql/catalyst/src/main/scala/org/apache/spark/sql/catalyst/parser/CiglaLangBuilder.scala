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

trait CiglaStatement extends Iterator[CiglaStatement] {
  // Advance accepts a func that can evaluate the statement.
  // The result of statement should be true or false. This should be enough for control flow (?).
  // Result can also be an exception. That is fine.
  // Result can also be function call. With functions things get a bit more tricky.
}

case class SparkStatement(command: String) extends CiglaStatement {
  // Execution is done outside...
  var consumed = false
  override def hasNext: Boolean = !consumed
  override def next(): CiglaStatement = {
    consumed = true
    this
  }
}

trait StatementBooleanEvaluator {
  def eval(statement: CiglaStatement): Boolean
}

case class CiglaIfElseStatement(
    condition: SparkStatement,
    ifBody: CiglaBody,
    elseBody: Option[CiglaBody],
    evaluator: StatementBooleanEvaluator) extends CiglaStatement {

  var executionList: List[CiglaStatement] =
    List(Some(condition), Some(ifBody), elseBody).flatten

  override def hasNext: Boolean = executionList.nonEmpty

  override def next(): CiglaStatement = {
    var curr = executionList.head
    val rem = executionList.tail

    // TODO: This is super ugly...
    executionList = curr.map {
        case s: SparkStatement =>
          val evalRes = evaluator.eval(s)
          if (evalRes) {
            List(rem.head)
          } else {
            rem.tail
          }
        case b: CiglaBody =>
          curr = b.next()
          if (b.hasNext) executionList else rem
        case _ => throw new RuntimeException("Invalid statement")
    }.toList.flatten
    curr
  }

}
case class CiglaBody(statements: ArrayBuffer[CiglaStatement]) extends CiglaStatement {
  private val statementIter = statements.iterator
  override def hasNext: Boolean = statementIter.hasNext
  override def next(): CiglaStatement = statementIter.next()
}

trait ProceduralLangInterface {
  def buildInterpreter(batch: String): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(batch: String): ProceduralLangInterpreter = CiglaLangInterpreter(batch)
}

trait ProceduralLangInterpreter extends Iterator[CiglaStatement]

case class CiglaLangInterpreter(batch: String) extends ProceduralLangInterpreter {
  // TODO: Keep parser here. We may need for error reporting - e.g. pointing to the
  // exact location of the error.

  // TODO: No need to init this every time..
  private val ciglaParser = new CiglaParser()
  private val parser = ciglaParser.parseBatch(batch)(t => t)

  private val astBuilder = CiglaLangBuilder(batch)
  private val tree = astBuilder.visitBody(parser.body())

  private val statements = tree.statements
  private val statementIter = statements.iterator

  override def hasNext: Boolean = statementIter.hasNext

  override def next(): CiglaStatement = {
    val stmt = statementIter.next()
    stmt
  }
}

//noinspection ScalaStyle
case class CiglaLangBuilder(batch: String) extends CiglaBaseParserBaseVisitor[AnyRef] {
  override def visitSparkStatement(
      ctx: CiglaBaseParser.SparkStatementContext): SparkStatement = {
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    SparkStatement(command)
  }

  override def visitBody(ctx: CiglaBaseParser.BodyContext): CiglaBody = {
    // TODO: Need to check type of statement here?
    val stmts = ctx.sparkStatement()
    CiglaBody(stmts.asScala.map(visitSparkStatement).asInstanceOf[ArrayBuffer[CiglaStatement]])
  }

  override def visitIfElseStatement(ctx: CiglaBaseParser.IfElseStatementContext): AnyRef =
    super.visitIfElseStatement(ctx)

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