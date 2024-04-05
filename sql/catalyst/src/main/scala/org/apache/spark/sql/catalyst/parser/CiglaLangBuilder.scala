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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait BatchPlanStatement

trait BatchStatementExec extends Logging {
  def rewind(): Unit
}

// Interpreter debugger can point to leaf statement.
// This can either be a Spark statement or Cigla statement (e.g. var assignment or trace).
// TODO: Add all debugging info here.
trait LeafStatementExec extends BatchStatementExec

// Non leaf statement hosts other statements. It can return iterator to it's children.
// It can also be executed multiple times (e.g. while loop).
abstract class NonLeafStatementExec
  extends BatchStatementExec with Iterator[BatchStatementExec]

// Statement that can be evaluated to a boolean.
// It can go in if/else condition or while loop.
trait BoolEvaluableStatement extends BatchPlanStatement with BatchStatementExec

// Statement that is supposed to be executed against Spark.
// Same object is used in both plan and execution.
case class SparkStatement(
    parsedPlan: LogicalPlan, sourceStart: Int, sourceEnd: Int)
    extends BatchPlanStatement with LeafStatementExec with BoolEvaluableStatement {
  // Execution can either be done outside
  // (e.g. you can just get command text and execute it locally).
  // Or internally (e.g. in case of SQL in IF branch.)
  // If Interpreter needs to execute it, it will set this to true.
  var consumed = false
  override def rewind(): Unit = consumed = false
  def getText(batch: String): String = batch.substring(sourceStart, sourceEnd)
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

case class BatchIfElseStatement(
  condition: BoolEvaluableStatement,
  ifBody: BatchBody,
  elseBody: Option[BatchBody]) extends BatchPlanStatement

case class BatchIfElseStatementExec(
    condition: BoolEvaluableStatement,
    ifBody: BatchBodyExec,
    elseBody: Option[CiglaLangNestedIteratorStatement],
    evaluator: Option[StatementBooleanEvaluator])
    extends NonLeafStatementExec {
  private object IfElseState extends Enumeration {
    val Condition, IfBody, ElseBody = Value
  }

  private var state = IfElseState.Condition
  private var curr: Option[BatchStatementExec] = Some(condition)

  override def rewind(): Unit = {
    state = IfElseState.Condition
    curr = Some(condition)
    condition.rewind()
    ifBody.rewind()
    elseBody.foreach(_.rewind())
  }

  override def hasNext: Boolean = curr.nonEmpty

  override def next(): BatchStatementExec = {
    assert(evaluator.nonEmpty, "Evaluator must be set for execution")
    state match {
      case IfElseState.Condition =>
        logInfo("Entering condition")
        assert(curr.get.isInstanceOf[SparkStatement])
        val evalRes = evaluator.get.eval(condition)
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

case class BatchWhileStatement(
  condition: BoolEvaluableStatement,
  whileBody: BatchBody) extends BatchPlanStatement

case class BatchWhileStatementExec(
    condition: BoolEvaluableStatement,
    whileBody: CiglaLangNestedIteratorStatement,
    evaluator: Option[StatementBooleanEvaluator])
    extends NonLeafStatementExec {
  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[BatchStatementExec] = Some(condition)

  override def hasNext: Boolean = curr.nonEmpty
  override def next(): BatchStatementExec = {
    assert(evaluator.nonEmpty, "Evaluator must be set for execution")
    state match {
      case WhileState.Condition =>
        if (evaluator.get.eval(condition)) {
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
case class CiglaLangNestedIteratorStatement(collection: List[BatchStatementExec])
    extends NonLeafStatementExec {

  var localIterator = collection.iterator
  var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

  // Called when a leaf statement is encountered.
  protected def processStatement(stmt: LeafStatementExec) = ()

  override def hasNext: Boolean = {
    val childHasNext = curr match {
      case Some(body: NonLeafStatementExec) => body.hasNext
      case Some(_: LeafStatementExec) => true
      case None => false
      case _ => throw new IllegalStateException("Unknown statement type")
    }
    val hasNext = localIterator.hasNext || childHasNext
    hasNext
  }
  override def next(): BatchStatementExec = {
    curr match {
      case None => throw new IllegalStateException("No more elements")
      case Some(stmt: LeafStatementExec) =>
        processStatement(stmt)
        if (localIterator.hasNext) curr = Some(localIterator.next())
        else curr = None
        stmt
      case Some(body: NonLeafStatementExec) =>
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

case class BatchBody(collection: List[BatchPlanStatement]) extends BatchPlanStatement

class BatchBodyExec(statements: List[BatchStatementExec])
    extends CiglaLangNestedIteratorStatement(statements)

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

trait ProceduralLangInterpreter extends Iterator[BatchStatementExec]

case class CiglaLangInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface)
    extends ProceduralLangInterpreter {
  private val treeNoEval = sparkStatementParser.parseBatch(batch)

  private def transformTreeIntoEvaluable(node: BatchPlanStatement): BatchStatementExec = {
    // Set evaluator where needed.
    node match {
      case body: BatchBody =>
        new BatchBodyExec(body.collection.map(stmt => transformTreeIntoEvaluable(stmt)))
      case whileStmt: BatchWhileStatement =>
        BatchWhileStatementExec(
          whileStmt.condition,
          new BatchBodyExec(
            whileStmt.whileBody.collection.map(stmt => transformTreeIntoEvaluable(stmt))),
          Some(evaluator))
      case ifStmt: BatchIfElseStatement =>
        BatchIfElseStatementExec(
          ifStmt.condition,
          new BatchBodyExec(ifStmt.ifBody.collection.map(stmt => transformTreeIntoEvaluable(stmt))),
          ifStmt.elseBody.map(elseBody =>
            new BatchBodyExec(elseBody.collection.map(stmt => transformTreeIntoEvaluable(stmt)))),
          Some(evaluator))
      case node: SparkStatement => node
      case _ => throw new IllegalStateException("Unknown statement type")
    }
  }

  private val tree = transformTreeIntoEvaluable(treeNoEval).asInstanceOf[BatchBodyExec]

  private val iter = new BatchBodyExec(tree.collection)
  override def hasNext: Boolean = iter.hasNext
  override def next(): BatchStatementExec = iter.next()
}