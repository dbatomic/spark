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

// TODO: Super hacky implementation. Just experimenting with the interfaces...

trait CiglaStatement extends Iterator[CiglaStatement] {
  // TODO: Figure out functional way to do this.
  // We should be just able to recreate iterators...
  def rewindToStart(): Unit
}

case class SparkStatement(command: String) extends CiglaStatement {
  // Execution can either be done outside
  // (e.g. you can just get command text and execute it locally).
  // Or internally (e.g. in case of SQL in IF branch.)
  // If Interpreter needs to execute it, it will set this to true.
  var consumed = false

  override def hasNext: Boolean = false
  override def next(): CiglaStatement = this
  override def rewindToStart(): Unit = consumed = false
}

// Provide a way to evaluate a statement to a boolean.
// Interpreter at this point only needs to know true/false
// result of statement. E.g. if it is part of branching condition.
// For var assignment, we will need more complex evaluator.
trait StatementBooleanEvaluator {
  def eval(statement: CiglaStatement): Boolean
}

case class CiglaIfElseStatement(
    condition: SparkStatement,
    ifBody: CiglaBody,
    elseBody: Option[CiglaBody],
    evaluator: StatementBooleanEvaluator) extends CiglaStatement {

  private object IfElseState extends Enumeration {
    val Condition, IfBody, ElseBody = Value
  }

  private var state = IfElseState.Condition
  private var curr: Option[CiglaStatement] = Some(condition)

  override def rewindToStart(): Unit = {
    state = IfElseState.Condition
    curr = Some(condition)
  }

  override def hasNext: Boolean = curr.nonEmpty

  override def next(): CiglaStatement = {
    // TODO: This is terrible...
    // Think about better abstraction.
    state match {
      case IfElseState.Condition =>
        assert(curr.get.isInstanceOf[SparkStatement])
        val evalRes = evaluator.eval(curr.get)
        curr.get.asInstanceOf[SparkStatement].consumed = true
        val ret = curr.get
        if (evalRes) {
          state = IfElseState.IfBody
          curr = Some(ifBody)
        } else {
          state = IfElseState.ElseBody
          curr = elseBody // Else can be None here
        }
        ret
      case IfElseState.IfBody =>
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
      case _ => throw new IllegalStateException("Invalid state")
    }
  }
}

case class CiglaWhileStatement(
   condition: SparkStatement,
   whileBody: CiglaBody,
   evaluator: StatementBooleanEvaluator) extends CiglaStatement {

  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[CiglaStatement] = Some(condition)

  override def hasNext: Boolean = curr.nonEmpty
  override def next(): CiglaStatement = {
    state match {
      case WhileState.Condition =>
        val toRet = curr.get
        curr.get.rewindToStart()
        val evalRes = evaluator.eval(curr.get)
        curr.get.asInstanceOf[SparkStatement].consumed = true
        if (evalRes) {
          // Need to rewind iterator in the body.
          state = WhileState.Body
          curr = Some(whileBody)
          whileBody.rewindToStart()
        } else {
          curr = None
        }
        toRet
      case WhileState.Body =>
        val ret = whileBody.next()
        if (!whileBody.hasNext) {
          // Go back to condition.
          state = WhileState.Condition
          curr = Some(condition)
        }
        ret
    }
  }

  override def rewindToStart(): Unit = {
    state = WhileState.Condition
    curr = Some(condition)
    condition.rewindToStart()
    whileBody.rewindToStart()
  }
}

// Nested iterator. This is a bit hacky, but it works for now.
// Idea is that top level iterator will proceed only after all nested iterators
// are exhausted.
// TODO: Figure out some functional way to do this...
class CiglaNestedIterator(var collection: Seq[CiglaStatement]) extends CiglaStatement {
  // curr is reactive iterator. It points to the element to be returned.
  // TODO: I don't really like this...

  private var iter = collection.iterator
  private var curr = if (iter.hasNext) Some(iter.next()) else None
  override def hasNext: Boolean = curr.nonEmpty
  override def next(): CiglaStatement = {
    var res = curr.get
    if (curr.get.hasNext) {
      // If current iterator has more elements, return next element.
      // But don't progress my iterator.
      res = curr.get.next()
    } else {
      // If current iterator is exhausted, move to next iterator.
      curr = if (iter.hasNext) Some(iter.next()) else None
    }
    res
  }

  override def rewindToStart(): Unit = {
    // is this going to work?
    // This is same as creating new nested iterator...
    // I need better design for this. For now just getting to work.

    // rewind every inner statement
    collection.foreach(_.rewindToStart())
    iter = collection.iterator
    curr = if (iter.hasNext) Some(iter.next()) else None
  }
}

case class CiglaBody(statements: List[CiglaStatement])
    extends CiglaNestedIterator(statements)

trait ProceduralLangInterface {
  def buildInterpreter(
    batch: String, evaluator: StatementBooleanEvaluator): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(
    batch: String, evaluator: StatementBooleanEvaluator): ProceduralLangInterpreter
    = CiglaLangInterpreter(batch, evaluator)
}

trait ProceduralLangInterpreter extends Iterator[CiglaStatement]

case class CiglaLangInterpreter(
    batch: String, evaluator: StatementBooleanEvaluator) extends ProceduralLangInterpreter {
  // TODO: Keep parser here. We may need for error reporting - e.g. pointing to the
  // exact location of the error.

  // TODO: No need to init this every time..
  private val ciglaParser = new CiglaParser()
  private val parser = ciglaParser.parseBatch(batch)(t => t)

  private val astBuilder = CiglaLangBuilder(batch, evaluator)
  private val tree = astBuilder.visitBody(parser.body())

  private val iter = new CiglaNestedIterator(tree.statements)

  override def hasNext: Boolean = iter.hasNext
  override def next(): CiglaStatement = iter.next()
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
    val buff = ListBuffer[CiglaStatement]()
    for (i <- 0 until ctx.getChildCount) {
      val child = ctx.getChild(i)
      val stmt = visit(child).asInstanceOf[CiglaStatement]
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