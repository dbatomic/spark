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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

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
  extends RewindableStatement with Iterator[RewindableStatement]

// Statement that can be evaluated to a boolean.
// It can go in if/else condition or while loop.
trait BoolEvaluableStatement extends RewindableStatement

// Statement that is supposed to be executed against Spark.
case class SparkStatement(
    command: String, parsedPlan: LogicalPlan, sourceStart: Int = 0, sourceEnd: Int = 0)
    extends LeafStatement with BoolEvaluableStatement {
  // Execution can either be done outside
  // (e.g. you can just get command text and execute it locally).
  // Or internally (e.g. in case of SQL in IF branch.)
  // If Interpreter needs to execute it, it will set this to true.
  var consumed = false
  override def rewind(): Unit = consumed = false
}

// Same as spark statement. Idea is to capture the place of definition and use it to track scope.
// The main point is to remove all the variables at the end of the given scope.
// E.g.:
// while (x < 10) {
//    declare y = 10;
//    set var y = y + 1;
//    // <- interpreter removes var declaration here ->
// }
// declare y = 22; // <-- this is now fine.
// Trickier situation will happen during procedure calls.
// Interpreter will have to remove all the variables in current scope
// and then restore them after the call.
case class CiglaVarDeclareStatement(
    varName: String, command: String, parsedPlan: LogicalPlan) extends LeafStatement {
  override def rewind(): Unit = {
    // TODO: Probably just remove the variable from the session of rewind?
  }
}

case object AlwaysTrueEval extends StatementBooleanEvaluator {
  override def eval(statement: BoolEvaluableStatement): Boolean = true
}

// Provide a way to evaluate a statement to a boolean.
// Interpreter at this point only needs to know true/false
// result of a statement. E.g. if it is part of branching condition.
// For var assignment, we will need more complex evaluator.
trait StatementBooleanEvaluator {
  def eval(statement: BoolEvaluableStatement): Boolean
}

case class CiglaIfElseStatement(
    condition: BoolEvaluableStatement,
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

  override def next(): RewindableStatement = {
    state match {
      case IfElseState.Condition =>
        logInfo("Entering condition")
        assert(curr.get.isInstanceOf[SparkStatement])
        val evalRes = evaluator.eval(condition)
        if (evalRes) {
          state = IfElseState.IfBody
          curr = Some(ifBody)
        } else {
          state = IfElseState.ElseBody
          curr = elseBody
        }
        condition
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
   condition: BoolEvaluableStatement,
   whileBody: CiglaLangNestedIteratorStatement,
   evaluator: StatementBooleanEvaluator) extends NonLeafStatement {
  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[RewindableStatement] = Some(condition)

  override def hasNext: Boolean = curr.nonEmpty
  override def next(): RewindableStatement = {
    state match {
      case WhileState.Condition =>
        if (evaluator.eval(condition)) {
          whileBody.rewind()
          state = WhileState.Body
          curr = Some(whileBody)
        } else {
          curr = None
        }
        condition
      case WhileState.Body =>
        val ret = whileBody.next()
        if (!whileBody.hasNext) {
          state = WhileState.Condition
          curr = Some(condition)
          condition.rewind()
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
class CiglaLangNestedIteratorStatement(collection: Seq[RewindableStatement])
    extends NonLeafStatement {

  var localIterator = collection.iterator
  var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

  // Called when a leaf statement is encountered.
  protected def processStatement(stmt: LeafStatement) = ()

  override def hasNext: Boolean = {
    val childHasNext = curr match {
      case Some(body: NonLeafStatement) => body.hasNext
      case Some(_: LeafStatement) => true
      case None => false
      case _ => throw new IllegalStateException("Unknown statement type")
    }
    val hasNext = localIterator.hasNext || childHasNext
    hasNext
  }
  override def next(): RewindableStatement = {
    curr match {
      case None => throw new IllegalStateException("No more elements")
      case Some(stmt: LeafStatement) =>
        processStatement(stmt)
        if (localIterator.hasNext) curr = Some(localIterator.next())
        else curr = None
        stmt
      case Some(body: NonLeafStatement) =>
        if (body.hasNext) {
          // progress body.
          body.next()
        } else {
          // progress my self
          curr = if (localIterator.hasNext) Some(localIterator.next()) else None
          next()
        }
      case _ => throw new IllegalStateException("Unknown statement type")
    }
  }

  override def rewind(): Unit = {
    collection.foreach(_.rewind())
    localIterator = collection.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }
}

case class CiglaBody(statements: List[RewindableStatement])
    extends CiglaLangNestedIteratorStatement(statements) {

  val variables = ListBuffer[String]()
  override def processStatement(stmt: LeafStatement): Unit = {
    stmt match {
      case CiglaVarDeclareStatement(varName, _, _) => variables += varName
      case _ =>
    }
  }

  override def rewind(): Unit = {
    super.rewind()
    variables.clear()
  }
}

trait ProceduralLangInterface {
  def buildInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface): ProceduralLangInterpreter
    = CiglaLangInterpreter(batch, evaluator, sparkStatementParser)
}

trait ProceduralLangInterpreter extends Iterator[RewindableStatement]

case class CiglaLangInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface)
    extends ProceduralLangInterpreter {
  private val tree = sparkStatementParser.parseBatch(batch)
  private val iter = CiglaBody(tree.statements)
  override def hasNext: Boolean = iter.hasNext
  override def next(): RewindableStatement = iter.next()
}

object CiglaLangBuilder {
  // Export friendly name.
  // Internally it is rewinding statement but from outside it is just a language statement.
  type CiglaLanguageStatement = RewindableStatement
}