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

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging

trait NodeStatement

// Interface suggesting that operation can be rewound.
// E.g. iterator can be set back to beginning if in while loop.
trait Rewindable {
  def rewind(): Unit
}

trait RewindableStatement extends NodeStatement with Rewindable with Logging

// Interpreter debugger can point to leaf statement.
// This can either be a Spark statement or Cigla statement (e.g. var assignment or trace).
// TODO: Add all debugging info here.
abstract class LeafStatement extends RewindableStatement

// Non leaf statement hosts other statements. It can return iterator to it's children.
// It can also be executed multiple times (e.g. while loop).
abstract class NonLeafStatement
  extends RewindableStatement with Iterator[Option[RewindableStatement]]

// Statement thtat is supposed to be executed against Spark.
case class SparkStatement(command: String) extends LeafStatement {
  // Execution can either be done outside
  // (e.g. you can just get command text and execute it locally).
  // Or internally (e.g. in case of SQL in IF branch.)
  // If Interpreter needs to execute it, it will set this to true.
  var consumed = false
  override def rewind(): Unit = consumed = false
}

// Provide a way to evaluate a statement to a boolean.
// Interpreter at this point only needs to know true/false
// result of a statement. E.g. if it is part of branching condition.
// For var assignment, we will need more complex evaluator.
trait StatementBooleanEvaluator {
  def eval(statement: SparkStatement): Boolean
}

case class CiglaIfElseStatement(
    condition: SparkStatement,
    ifBody: CiglaBody,
    elseBody: Option[CiglaBody],
    evaluator: StatementBooleanEvaluator) extends NonLeafStatement {
  private object IfElseState extends Enumeration {
    val Condition, IfBody, ElseBody = Value
  }

  private var state = IfElseState.Condition
  private var curr: Option[RewindableStatement] = Some(condition)

  override def rewind(): Unit = {
    state = IfElseState.Condition
    curr = Some(condition)
    condition.rewind()
    ifBody.rewind()
    elseBody.foreach(_.rewind())
  }

  override def hasNext: Boolean = curr.nonEmpty

  override def next(): Option[RewindableStatement] = {
    state match {
      case IfElseState.Condition =>
        logInfo("Entering condition")
        assert(curr.get.isInstanceOf[SparkStatement])
        val evalRes = evaluator.eval(condition)
        condition.consumed = true
        if (evalRes) {
          state = IfElseState.IfBody
          curr = Some(ifBody)
        } else {
          state = IfElseState.ElseBody
          curr = elseBody
        }
        Some(condition)
      case IfElseState.IfBody =>
        logInfo("Entering body")
        val ret = ifBody.next()
        if (!ifBody.hasNext) {
          curr = None
        }
        ret
      case IfElseState.ElseBody =>
        val ret = elseBody.get.next()
        if (!elseBody.get.hasNext) {
          curr = None
        }
        ret
    }
  }
}

case class CiglaWhileStatement(
   condition: SparkStatement,
   whileBody: CiglaBody,
   evaluator: StatementBooleanEvaluator) extends NonLeafStatement {
  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[RewindableStatement] = Some(condition)

  override def hasNext: Boolean = curr.nonEmpty
  override def next(): Option[RewindableStatement] = {
    state match {
      case WhileState.Condition =>
        condition.consumed = true
        if (evaluator.eval(condition)) {
          whileBody.rewind()
          state = WhileState.Body
          curr = Some(whileBody)
        } else {
          curr = None
        }
        Some(condition)
      case WhileState.Body =>
        val ret = whileBody.next()
        if (!whileBody.hasNext) {
          state = WhileState.Condition
          curr = Some(condition)
        }
        ret
    }
  }

  override def rewind(): Unit = {
    state = WhileState.Condition
    curr = Some(condition)
    condition.rewind()
    whileBody.rewind()
  }
}

// This is base class for all nested cigla lang statements.
// e.g. if/else/while or even regular body.
class CiglaLangNestedIteratorStatement(val collection: Seq[RewindableStatement])
    extends NonLeafStatement {

  var localIterator = collection.iterator
  var curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  override def hasNext: Boolean = curr.nonEmpty
  override def next(): Option[RewindableStatement] = {
    curr match {
      case None => None
      case Some(stmt: LeafStatement) =>
        if (!localIterator.hasNext) curr = None
        else curr = Some(localIterator.next())
        Some(stmt)
      case Some(body: NonLeafStatement) =>
        if (body.hasNext) {
          // keep body.
          body.next()
        } else {
          if (localIterator.hasNext) {
            curr = Some(localIterator.next())
            next()
          } else {
            // We are done.
            curr = None
            None
          }
        }
      case _ => throw new IllegalStateException("Statement not supported")
    }
  }

  override def rewind(): Unit = {
    collection.foreach(_.rewind())
    localIterator = collection.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }
}

case class CiglaBody(statements: List[RewindableStatement])
    extends CiglaLangNestedIteratorStatement(statements)

trait ProceduralLangInterface {
  def buildInterpreter(
    batch: String, evaluator: StatementBooleanEvaluator): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(
    batch: String, evaluator: StatementBooleanEvaluator): ProceduralLangInterpreter
    = CiglaLangInterpreter(batch, evaluator)
}

trait ProceduralLangInterpreter extends Iterator[Option[RewindableStatement]]

case class CiglaLangInterpreter(batch: String, evaluator: StatementBooleanEvaluator)
    extends ProceduralLangInterpreter {
  // TODO: Keep parser here. We may need for error reporting - e.g. pointing to the
  // exact location of the error.
  // TODO: No need to init this every time..
  private val ciglaParser = new CiglaParser()
  private val parser = ciglaParser.parseBatch(batch)(t => t)

  private val astBuilder = CiglaLangBuilder(batch, evaluator)
  private val tree = astBuilder.visitBody(parser.body())

  private val iter = new CiglaLangNestedIteratorStatement(tree.statements)

  override def hasNext: Boolean = iter.hasNext

  override def next(): Option[RewindableStatement] = iter.next()
}

//noinspection ScalaStyle
case class CiglaLangBuilder(batch: String, evaluator: StatementBooleanEvaluator)
  extends CiglaBaseParserBaseVisitor[AnyRef] {
  override def visitSparkStatement(
      ctx: CiglaBaseParser.SparkStatementContext): SparkStatement = {
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    // We can choose to parse the command here and get AST.
    // AST should be cacheable.
    // For now we are keeping raw string.
    SparkStatement(command)
  }

  override def visitBody(ctx: CiglaBaseParser.BodyContext): CiglaBody = {
    val buff = ListBuffer[RewindableStatement]()
    for (i <- 0 until ctx.getChildCount) {
      val child = ctx.getChild(i)
      val stmt = visit(child).asInstanceOf[RewindableStatement]
      buff += stmt
    }
    CiglaBody(buff.toList)
  }

  override def visitIfElseStatement(
      ctx: CiglaBaseParser.IfElseStatementContext): CiglaIfElseStatement = {
    val condition = visitSparkStatement(ctx.sparkStatement())
    // TODO: Some more work is needed for else if.
    val ifBody = visitBody(ctx.body(0))
    val elseBody = Option(ctx.body(1)).map(visitBody)
    CiglaIfElseStatement(condition, ifBody, elseBody, evaluator)
  }

  override def visitWhileStatement(
      ctx: CiglaBaseParser.WhileStatementContext): CiglaWhileStatement = {
    val condition = visitSparkStatement(ctx.sparkStatement())
    val whileBody = visitBody(ctx.body)
    CiglaWhileStatement(condition, whileBody, evaluator)
  }
}

object CiglaLangBuilder {
  // Export friendly name.
  // Internally it is rewinding statement but from outside it is just a language statement.
  type CiglaLanguageStatement = RewindableStatement
}

