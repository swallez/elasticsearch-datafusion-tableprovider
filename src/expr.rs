use datafusion::common::{Column, ScalarValue};
use datafusion::error::DataFusionError;

use datafusion::logical_expr::*;

type ExprResult = Result<(), DataFusionError>;

pub fn add_expr(expr: &Expr, mut depth: usize, esql: &mut String) -> ExprResult {
    use Expr::*;

    depth = depth + 1;

    match expr {
        BinaryExpr(b) => add_binary_expr(b, depth, esql),
        Literal(l) => add_literal(l, depth, esql),
        Column(c) => add_column(c, depth, esql),
        Alias(_) |
        ScalarVariable(_, _) |
        Like(_) |
        SimilarTo(_) |
        Not(_) |
        IsNotNull(_) |
        IsNull(_) |
        IsTrue(_) |
        IsFalse(_) |
        IsUnknown(_) |
        IsNotTrue(_) |
        IsNotFalse(_) |
        IsNotUnknown(_) |
        Negative(_) |
        Between(_) |
        Case(_) |
        Cast(_) |
        TryCast(_) |
        Sort(_) |
        ScalarFunction(_) |
        AggregateFunction(_) |
        WindowFunction(_) |
        InList(_) |
        Exists(_) |
        InSubquery(_) |
        ScalarSubquery(_) |
        Wildcard { .. } |
        GroupingSet(_) |
        Placeholder(_) |
        OuterReferenceColumn(_, _) |
        Unnest(_) => Err(DataFusionError::NotImplemented(format!("Filter {expr:?} is not implemented")))
    }
}

fn add_column(c: &Column, _depth: usize, esql: &mut String) -> Result<(), DataFusionError> {
    // Note: DF ensures the column belongs to this table provider. In the context of ES
    // we only care about the table name which is the index name.
    
    esql.push('`'); // Always escape column names
    esql.push_str(&c.name);
    esql.push('`');
    Ok(())
}

fn add_literal(l: &ScalarValue, _depth: usize, esql: &mut String) -> ExprResult {
    use ScalarValue::*;

    match l {
        Utf8(Some(s)) => {
            esql.push('"');
            esql.push_str(s); // FIXME: needs to be escaped
            esql.push('"');
        }
        Null => esql.push_str("NULL"),
        lit => esql.push_str(&lit.to_string())
    }

    Ok(())
}

fn add_binary_expr(expr: &BinaryExpr, depth: usize, esql: &mut String) -> ExprResult {

    if depth > 1 {
        esql.push('(');
    }
    add_expr(&expr.left, depth, esql)?;
    add_op(&expr.op, depth, esql)?;
    add_expr(&expr.right, depth, esql)?;
    if depth > 1 {
        esql.push(')');
    }

    Ok(())
}

fn add_op(op: &Operator, _depth: usize, esql: &mut String) -> ExprResult {
    use Operator::*;

    // Missing: is null, is not null, cast, in,

    let op_str = match op {
        Eq => " == ",
        NotEq => " != ",
        Lt => " < ",
        LtEq => " <= ",
        Gt => " > ",
        GtEq => " >= ",
        Plus => " + ",
        Minus => " - ", // also unary
        Multiply => " * ",
        Divide => " / ",
        Modulo => " % ",
        And => " AND ",
        Or => " OR ",
        LikeMatch => " LIKE ",
        RegexMatch => " RLIKE ",
        StringConcat => " + ",

        IsDistinctFrom |
        IsNotDistinctFrom |
        RegexIMatch |
        RegexNotMatch |
        RegexNotIMatch |
        ILikeMatch |
        NotLikeMatch |
        NotILikeMatch |
        BitwiseAnd |
        BitwiseOr |
        BitwiseXor |
        BitwiseShiftRight |
        BitwiseShiftLeft |
        AtArrow |
        ArrowAt => return Err(DataFusionError::NotImplemented(format!("Operator {op:?} is not implemented")))
    };

    esql.push_str(op_str);

    Ok(())
}


