package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType}

object Transforms {

  // Check whether a ScalaUDF Expression is our dist UDF
  def isDistUdf(udf: ScalaUDF): Boolean = {
    udf.udfName.getOrElse("") == "dist"
  }

  // Get an Expression representing the dist_sq UDF with the provided
  // arguments
  def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Double, y1: Double, x2: Double, y2: Double) => {
        val xDiff = x1 - x2
        val yDiff = y1 - y2
        xDiff * xDiff + yDiff * yDiff
      }, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
      udfName = Some("dist_sq"))
  }

  def getDistSqUdfInt(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Integer, y1: Integer, x2: Integer, y2: Integer) => {
        val xDiff = x1 - x2
        val yDiff = y1 - y2
        xDiff * xDiff + yDiff * yDiff
      }, IntegerType, args, Seq(IntegerType, IntegerType, IntegerType, IntegerType),
      udfName = Some("dist_sq"))
  }

  // Return any additional optimization passes here
  def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(EliminateZeroDists(spark), ElimnateNegativeConstants(spark), ElimnateNegativeConstantsEq(spark), SquaringDistLiteral(spark), SquaringDistBothExp(spark))
  }

  case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
        udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
    }
  }

  case class ElimnateNegativeConstants(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case LessThan(scalaUDF: ScalaUDF, Literal(c:Double, DoubleType)) if isDistUdf(scalaUDF) => Literal(false, BooleanType)
      case GreaterThanOrEqual(Literal(c:Double, DoubleType), scalaUDF: ScalaUDF) if isDistUdf(scalaUDF) && (c<0.0) => Literal(false, BooleanType)
    }
  }

  case class ElimnateNegativeConstantsEq(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case EqualTo(scalaUDF: ScalaUDF, Literal(-1, DoubleType)) if isDistUdf(scalaUDF) => Literal(false, BooleanType)
    }
  }

  case class SquaringDistLiteral(spark: SparkSession) extends Rule[LogicalPlan]{
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case GreaterThan(scalaUDF: ScalaUDF, Literal(c:Double, DoubleType)) if isDistUdf(scalaUDF) => GreaterThan(getDistSqUdf(scalaUDF.children), Literal(c*c, DoubleType));
      case GreaterThan(Literal(c:Double, DoubleType), scalaUDF: ScalaUDF) if isDistUdf(scalaUDF) => GreaterThan(Literal(c*c, DoubleType), getDistSqUdf(scalaUDF.children));
    }
  }

  case class SquaringDistBothExp(spark: SparkSession) extends Rule[LogicalPlan]{
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case GreaterThan(udf1: ScalaUDF, udf2:ScalaUDF) if isDistUdf(udf1) && isDistUdf(udf2) => GreaterThan(getDistSqUdf(udf1.children), getDistSqUdf(udf2.children));
    }
  }

}
