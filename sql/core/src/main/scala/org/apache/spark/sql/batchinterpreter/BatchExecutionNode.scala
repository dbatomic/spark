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
package org.apache.spark.sql.batchinterpreter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.BooleanType

sealed trait BatchStatementExec extends Logging {
  def rewind(): Unit
  val isInternal: Boolean = false
}

// Interpreter debugger can point to leaf statement.
// This can either be a Spark statement or Batch statement (e.g. var assignment or trace).
// TODO: Add all debugging info here.
trait LeafStatementExec extends BatchStatementExec

// Non leaf statement hosts other statements. It can return iterator to it's children.
// It can also be executed multiple times (e.g. while loop).
abstract class NonLeafStatementExec
  extends BatchStatementExec with Iterator[BatchStatementExec]

// Statement that is supposed to be executed against Spark.
// Same object is used in both plan and execution.
case class SparkStatementWithPlanExec(
  parsedPlan: LogicalPlan, sourceStart: Int, sourceEnd: Int, internal: Boolean)
    extends LeafStatementExec {
  // Execution can either be done outside
  // (e.g. you can just get command text and execute it locally).
  // Or internally (e.g. in case of SQL in IF branch.)
  // If Interpreter needs to execute it, it will set this to true.
  var consumed = false
  override def rewind(): Unit = consumed = false
  override val isInternal: Boolean = internal
  def getText(batch: String): String = batch.substring(sourceStart, sourceEnd)
}

// This is base class for all nested lang statements.
// e.g. if/else/while or even regular body.
case class BatchNestedIteratorStatementExec(collection: List[BatchStatementExec])
  extends NonLeafStatementExec {

  var localIterator = collection.iterator
  var curr = if (localIterator.hasNext) Some(localIterator.next()) else None

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

class BatchBodyExec(statements: List[BatchStatementExec])
  extends BatchNestedIteratorStatementExec(statements)
case class BatchIfElseStatementExec(
    condition: LeafStatementExec,
    ifBody: BatchBodyExec,
    elseBody: Option[BatchNestedIteratorStatementExec],
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
        assert(curr.get.isInstanceOf[SparkStatementWithPlanExec])
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

case class BatchWhileStatementExec(
    condition: LeafStatementExec,
    whileBody: BatchNestedIteratorStatementExec,
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


// Evaluators:

// Provide a way to evaluate a statement to a boolean.
// Interpreter at this point only needs to know true/false
// result of a statement. E.g. if it is part of branching condition.
trait StatementBooleanEvaluator {
  def eval(statement: LeafStatementExec): Boolean
}

case class DataFrameEvaluator(session: SparkSession) extends StatementBooleanEvaluator {
  override def eval(statement: LeafStatementExec): Boolean = statement match {
    case stmt: SparkStatementWithPlanExec =>
      assert(!stmt.consumed)
      stmt.consumed = true
      val df = Dataset.ofRows(session, stmt.parsedPlan)

      // Rules to check whether this dataframe evaluates to true:
      // True only if single row with single column of a boolean type
      // with value TRUE.
      (df.count(), df.schema.fields) match {
        case (1, Array(field)) if field.dataType == BooleanType =>
          df.collect()(0).getBoolean(0)
        case _ => false
      }
    case _ => false
  }
}
