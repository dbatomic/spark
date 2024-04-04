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
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.types.BooleanType

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

case class SparkExpression(
    command: String,
    parsedPlan: Expression,
    sourceStart: Int = 0,
    sourceEnd: Int = 0) extends LeafStatement with BoolEvaluableStatement {
  var consumed = false
  override def rewind(): Unit = consumed = true
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
  // TODO: Keep parser here. We may need for error reporting - e.g. pointing to the
  // exact location of the error.
  // TODO: No need to init this every time..
  private val ciglaParser = new CiglaParser()
  private val parser = ciglaParser.parseBatch(batch)(t => t)

  private val astBuilder =
    CiglaLangBuilder(batch, evaluator, sparkStatementParser)
  private val tree = astBuilder.visitBody(parser.body())

  private val iter = CiglaBody(tree.statements)

  override def hasNext: Boolean = iter.hasNext

  override def next(): RewindableStatement = iter.next()
}

//noinspection ScalaStyle
case class CiglaLangBuilder(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface)
    extends CiglaBaseParserBaseVisitor[AnyRef] {
  override def visitSparkStatement(
      ctx: CiglaBaseParser.SparkStatementContext): SparkStatement = {
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    // We can choose to parse the command here and get AST.
    // AST should be cacheable.
    // For now we are keeping raw string.
    val parsedPlan = sparkStatementParser.parsePlan(command)
    // TODO: Add debug info here as well.
    SparkStatement(command, parsedPlan)
  }

  override def visitBody(ctx: CiglaBaseParser.BodyContext): CiglaBody = {
    val buff = ListBuffer[RewindableStatement]()
    for (i <- 0 until ctx.getChildCount) {
      val child = ctx.getChild(i)
      val stmt = visit(child).asInstanceOf[RewindableStatement]
      // TODO: This is a hack. For some reason ';' is parsed as a statement.
      // Just ignoring it here. Figure out later what is going on.
      if (stmt != null) {
        buff += stmt
      }
    }

    // add all remove variable statements for any defined variables in this block.
    val dropCommands = buff.filter(_.isInstanceOf[CiglaVarDeclareStatement]).map { stmt =>
      val varName = stmt.asInstanceOf[CiglaVarDeclareStatement].varName
      val command = "DROP TEMPORARY VARIABLE " + varName
      val parsedPlan = sparkStatementParser.parsePlan(command)
      SparkStatement(command, parsedPlan)
    }
    CiglaBody(buff.toList ++ dropCommands.reverse)
  }

  override def visitIfElseStatement(
      ctx: CiglaBaseParser.IfElseStatementContext): CiglaIfElseStatement = {

    val condition = if (ctx.boolStatementOrExpression().sparkStatement() != null) {
      visitSparkStatement(ctx.boolStatementOrExpression().sparkStatement())
    } else if (ctx.boolStatementOrExpression().expression() != null) {
      visitExpression(ctx.boolStatementOrExpression().expression())
    } else {
      throw new IllegalStateException("Only spark statement and expression are supported.")
    }
    // TODO: Some more work is needed for else if.
    val ifBody = visitBody(ctx.body(0))
    val elseBody = Option(ctx.body(1)).map(visitBody)
    CiglaIfElseStatement(condition, ifBody, elseBody, evaluator)
  }

  override def visitExpression(ctx: CiglaBaseParser.ExpressionContext): SparkStatement = {
    // this is same as visit spark statement for now.
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    // We can choose to parse the command here and get AST.
    // AST should be cacheable.
    // For now we are keeping raw string.
    val expression = sparkStatementParser.parseExpression(command)

    expression.dataType match {
        case _: BooleanType => // do nothing
        case _ => throw new IllegalStateException("Only boolean expressions are supported.")
        // TODO: What are the other rules? E.g. we could say that int != 0 is same as true?
        // We can do automatic cast to boolean here as well?
    }

    // We build fake logical plan here that is a simple projection against given expression.
    val plan = Project(Seq(Alias(expression, "condition")()), OneRowRelation())

    // TODO: Add debug info here as well.
    SparkStatement(command, plan)
  }

  override def visitWhileStatement(
      ctx: CiglaBaseParser.WhileStatementContext): CiglaWhileStatement = {
    val condition = if (ctx.boolStatementOrExpression().sparkStatement() != null) {
      visitSparkStatement(ctx.boolStatementOrExpression().sparkStatement())
    } else if (ctx.boolStatementOrExpression().expression() != null) {
      visitExpression(ctx.boolStatementOrExpression().expression())
    } else {
      throw new IllegalStateException("Only spark statement and expression are supported.")
    }

    val whileBody = visitBody(ctx.body)
    CiglaWhileStatement(condition, whileBody, evaluator)
  }

  override def visitDeclareVar(ctx: CiglaBaseParser.DeclareVarContext): CiglaVarDeclareStatement = {
    // this is just a proxy to track variable lifetime.
    // all the real work is done in the spark expression.
    val varName = ctx.varName.getText
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val originalStatement = batch.substring(start, stop + 1)
    val sparkPlan = sparkStatementParser.parsePlan(originalStatement)

    CiglaVarDeclareStatement(varName, originalStatement, sparkPlan)
  }
}

object CiglaLangBuilder {
  // Export friendly name.
  // Internally it is rewinding statement but from outside it is just a language statement.
  type CiglaLanguageStatement = RewindableStatement
}