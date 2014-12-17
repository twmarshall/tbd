package tbd.sql;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SubSelect;


public class Evaluator implements ExpressionVisitor {
  private Object accumulator;
  private boolean accumulatorBoolean;
  private Column columnValue;
  private Boolean firstEntry;

  Datum[] t;
  List<String> tupleTableMap;

  public Object getResult() {
    return accumulator;
  }

  public Evaluator(Datum[] t2) {
    t = t2;
    firstEntry=null;
    List<String> tupleTableMap = null;
  }

  public Evaluator(Datum[] t2, List<String> tupleTableMap) {
    this(t2);
    this.tupleTableMap = tupleTableMap;
  }

  @Override
    public void visit(Addition addition) {
    if(firstEntry==null){
      firstEntry=true;
      columnValue = new Column(null, addition.toString());
    }
    addition.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    addition.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof Double && rightValue instanceof Double) {
      accumulator = (Double)leftValue        + (Double)rightValue;
      accumulator = (Double)accumulator*100/100;

    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      accumulator = (Long)leftValue + (Long)rightValue;
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      accumulator = (Double)leftValue        + ((Long)rightValue).doubleValue();
      accumulator = (Double)accumulator*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      accumulator = ((Long)leftValue).doubleValue()        + (Double)rightValue;
      accumulator = (Double)accumulator*100/100;
    }
  }

  public Column getColumn(){
    return columnValue;
  }

  public void visit(Column column) {
    int index=-1;
    if(firstEntry==null){
      columnValue = column;
    }
    String columnName = column.getWholeColumnName().toLowerCase();
    if(tupleTableMap.contains(columnName)) {
      index = tupleTableMap.indexOf(columnName);
    }

    Datum row = t[index];

    if (row instanceof Datum.dLong) {
      accumulator = ((Datum.dLong) row).getValue();
    } else if (row instanceof Datum.dDate) {
      accumulator = ((Datum.dDate) row).getValue();
    } else if (row instanceof Datum.dString) {
      accumulator = ((Datum.dString) row).getValue();
    } else if (row instanceof Datum.dDecimal) {
      accumulator = ((Datum.dDecimal) row).getValue();
    }
  }


  @Override
    public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    boolean leftValue = accumulatorBoolean;
    if(leftValue==false){
      return;
    }
    andExpression.getRightExpression().accept(this);
    boolean rightValue = accumulatorBoolean;
    if (leftValue && rightValue) {
      accumulatorBoolean = true;
    } else {
      accumulatorBoolean = false;
    }
  }

  @Override
    public void visit(Between between) {
    between.getLeftExpression().accept(this);
    between.getBetweenExpressionStart().accept(this);
    between.getBetweenExpressionEnd().accept(this);
  }

  @Override
    public void visit(Division division) {
    if(firstEntry==null){
      firstEntry=true;
      columnValue = new Column(null, division.toString());
    }
    division.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    division.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof Double && rightValue instanceof Double) {
      accumulator = (Double)leftValue / (Double)rightValue;
      accumulator = Double.parseDouble(accumulator.toString())*100/100;

    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      accumulator = (Long)leftValue / (Long)rightValue;
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      accumulator = (Double)leftValue        / ((Long)rightValue).doubleValue();
      accumulator = (Double)accumulator*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      accumulator = ((Long)leftValue).doubleValue()        / (Double)rightValue;
      accumulator = (Double)accumulator*100/100;
    }
  }

  @Override
    public void visit(DoubleValue doubleValue) {
    accumulator = doubleValue.getValue()*100/100;
  }

  @Override
    public void visit(EqualsTo equalsTo) {
    accumulatorBoolean = false;
    equalsTo.getLeftExpression().accept(this);
    Object leftValue = accumulator;

    equalsTo.getRightExpression().accept(this);
    Object rightValue = accumulator;

    if (leftValue instanceof String && rightValue instanceof String) {
      if (leftValue.equals(rightValue)) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      if (((Double)leftValue).compareTo((Double)rightValue) == 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if (((Long)leftValue).compareTo((Long)rightValue) == 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (date1.equals(date2)) {
        accumulatorBoolean = true;
      }
    }
  }

  @Override
  public void visit(Function function) {

    String functionName = function.getName().toLowerCase();
    ExpressionList parameters = function.getParameters();

    List expressionList = null;
    if(parameters!=null){
      expressionList = parameters.getExpressions();
    }
    java.lang.reflect.Method method=null;
    try {
      method = this.getClass().getMethod(functionName, List.class);
    } catch (SecurityException e) {
      // ...
    } catch (NoSuchMethodException e) {
    }
    try {
      method.invoke(this, expressionList);
    } catch (IllegalArgumentException e) {
    } catch (IllegalAccessException e) {
    } catch (InvocationTargetException e) {
    }
  }

  public void sum(List eList) {
    Evaluator ct = new Evaluator(this.t, this.tupleTableMap);
    Expression e = (Expression) eList.get(0);
    columnValue = new Column(null, e.toString());
    e.accept(ct);

    this.accumulator = ct.getResult();
  }

  public void avg(List eList) {
    Evaluator ct = new Evaluator(this.t, this.tupleTableMap);
    Expression e = (Expression) eList.get(0);
    columnValue = new Column(null, e.toString());
    e.accept(ct);
    this.accumulator = ct.getResult();
  }

  public void count(List eList) {
    this.accumulator = Long.parseLong(String.valueOf(1));;
  }

  public void min(List eList) {
    Evaluator ct = new Evaluator(this.t, this.tupleTableMap);

    //Assuming only one date is passed
    //Hence index is 0
    Expression e = (Expression) eList.get(0);
    columnValue = new Column(null, e.toString());
    e.accept(ct);

    this.accumulator = ct.getResult();
  }

  public void max(List eList) {
    Evaluator ct = new Evaluator(this.t, this.tupleTableMap);
    //Assuming only one date is passed
    //Hence index is 0
    Expression e = (Expression) eList.get(0);
    columnValue = new Column(null, e.toString());
    e.accept(ct);
    this.accumulator = ct.getResult();
  }

  public void date(List eList) {
    Evaluator ct = new Evaluator(this.t, this.tupleTableMap);
    Date date = null;
    //Assuming only one date is passed
    //Hence index is 0
    Expression e = (Expression) eList.get(0);
    e.accept(ct);
    this.accumulator = ct.getResult();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      date = dateFormat.parse((String) this.accumulator);
    } catch (ParseException e1) {
      e1.printStackTrace();
    }
    this.accumulator = date;
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    accumulatorBoolean = false;
    greaterThan.getLeftExpression().accept(this);
    Object leftValue = accumulator;

    greaterThan.getRightExpression().accept(this);
    Object rightValue = accumulator;

    if (leftValue instanceof String && rightValue instanceof String) {
      if (leftValue.toString().compareTo(rightValue.toString()) > 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      Double d1 = (Double)leftValue;
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison>0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if ((Long)leftValue > (Long)rightValue) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      Double d1 = ((Long)leftValue).doubleValue();
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison>0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      Double d1 = (Double)leftValue;
      Double d2 = ((Long)rightValue).doubleValue();
      int comparison = Double.compare(d1, d2);
      if (comparison>0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (date1.compareTo(date2)>0) {
        accumulatorBoolean = true;
      }
    } else {
    }
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    accumulatorBoolean = false;
    greaterThanEquals.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    greaterThanEquals.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof String && rightValue instanceof String) {
      if (leftValue.toString().compareTo(rightValue.toString()) >= 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      Double d1 = (Double)leftValue;
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison>=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if ((Long)leftValue >= (Long)rightValue) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      Double d1 = ((Long)leftValue).doubleValue();
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison>=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      Double d1 = (Double)leftValue;
      Double d2 = ((Long)rightValue).doubleValue();
      int comparison = Double.compare(d1, d2);
      if (comparison>=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (date1.compareTo(date2)>=0) {
        accumulatorBoolean = true;
      }
    } else {
    }
  }

  @Override
  public void visit(InverseExpression inverseExpression) {
    inverseExpression.getExpression().accept(this);
  }

  @Override
  public void visit(IsNullExpression isNullExpression) {
  }

  @Override
  public void visit(JdbcParameter jdbcParameter) {
  }

  @Override
  public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {
  }

  @Override
  public void visit(LikeExpression likeExpression) {
    visitBinaryExpression(likeExpression);
  }

  @Override
  public void visit(ExistsExpression existsExpression) {
    existsExpression.getRightExpression().accept(this);
  }

  @Override
  public void visit(LongValue longValue) {
    accumulator = longValue.getValue();
  }

  public void visit(Object longValue) {
    accumulator = longValue;
  }

  public void visit(net.sf.jsqlparser.expression.IntervalExpression intveral) {}

  public void visit(net.sf.jsqlparser.expression.ExtractExpression ext){}

  public void visit(net.sf.jsqlparser.expression.AnalyticExpression anaylyic) {}

  @Override
  public void visit(MinorThan minorThan) {
    accumulatorBoolean = false;
    minorThan.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    minorThan.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof String && rightValue instanceof String) {
      if (leftValue.toString().compareTo(rightValue.toString()) < 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      Double d1 = (Double)leftValue;
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison<0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if ((Long)leftValue < (Long)rightValue) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      Double d1 = ((Long)leftValue).doubleValue();
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison<0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      Double d1 = (Double)leftValue;
      Double d2 = ((Long)rightValue).doubleValue();
      int comparison = Double.compare(d1, d2);
      if (comparison<0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (date1.compareTo(date2)<0) {
        accumulatorBoolean = true;
      }
    } else {
    }
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    accumulatorBoolean = false;
    minorThanEquals.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    minorThanEquals.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof String && rightValue instanceof String) {
      if (leftValue.toString().compareTo(rightValue.toString()) <= 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      Double d1 = (Double)leftValue;
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison<=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if ((Long)leftValue <= (Long)rightValue) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      Double d1 = ((Long)leftValue).doubleValue();
      Double d2 = (Double)rightValue;
      int comparison = Double.compare(d1, d2);
      if (comparison<=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      Double d1 = (Double)leftValue;
      Double d2 = ((Long)rightValue).doubleValue();
      int comparison = Double.compare(d1, d2);
      if (comparison<=0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (date1.compareTo(date2)<=0) {
        accumulatorBoolean = true;
      }
    } else {
    }
  }

  @Override
  public void visit(Multiplication multiplication) {
    if(firstEntry==null){
      firstEntry=true;
      columnValue = new Column(null, multiplication.toString());
    }
    multiplication.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    multiplication.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof Double && rightValue instanceof Double) {
      accumulator = (Double)leftValue
        * (Double)rightValue;
      accumulator = Double.parseDouble(accumulator.toString())*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      accumulator = (Long)leftValue
        * (Long)rightValue;
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      accumulator = (Double)leftValue
        * ((Long)rightValue).doubleValue();
      accumulator = (Double)accumulator*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      accumulator = ((Long)leftValue).doubleValue()
        * (Double)rightValue;
      accumulator = (Double)accumulator*100/100;
    }
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    accumulatorBoolean = false;
    notEqualsTo.getLeftExpression().accept(this);
    Object leftValue = accumulator;
    notEqualsTo.getRightExpression().accept(this);
    Object rightValue = accumulator;
    if (leftValue instanceof String && rightValue instanceof String) {
      if (!leftValue.equals(rightValue)) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Double && rightValue instanceof Double) {
      if (((Double)leftValue).compareTo((Double)rightValue) != 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      if (((Long)leftValue).compareTo((Long)rightValue) != 0) {
        accumulatorBoolean = true;
      }
    } else if (leftValue instanceof Date && rightValue instanceof Date) {
      Date date1 = (Date) leftValue;
      Date date2 = (Date) rightValue;
      if (!date1.equals(date2)) {
        accumulatorBoolean = true;
      }
    }
  }

  @Override
  public void visit(NullValue nullValue) {
  }

  @Override
  public void visit(OrExpression orExpression) {
    orExpression.getLeftExpression().accept(this);
    boolean leftValue = accumulatorBoolean;
    if(leftValue==true){
      return;
    }
    orExpression.getRightExpression().accept(this);
    boolean rightValue = accumulatorBoolean;
    if (leftValue || rightValue) {
      accumulatorBoolean = true;
    } else {
      accumulatorBoolean = false;
    }
  }

  @Override
  public void visit(Parenthesis parenthesis) {
    parenthesis.getExpression().accept(this);
  }

  @Override
  public void visit(StringValue stringValue) {
    accumulator = stringValue.getValue();
  }

  @Override
  public void visit(Subtraction subtraction) {
    if(firstEntry==null){
      firstEntry=true;
      columnValue = new Column(null, subtraction.toString());
    }
    subtraction.getLeftExpression().accept(this);
    Object leftValue = accumulator;

    subtraction.getRightExpression().accept(this);
    Object rightValue = accumulator;

    if (leftValue instanceof Double && rightValue instanceof Double) {
      accumulator = (Double)leftValue
        - (Double)rightValue;
      accumulator = (Double)accumulator*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Long) {
      accumulator = (Long)leftValue
        - (Long)rightValue;
    } else if (leftValue instanceof Double && rightValue instanceof Long) {
      accumulator = (Double)leftValue
        - ((Long)rightValue).doubleValue();
      accumulator = (Double)accumulator*100/100;
    } else if (leftValue instanceof Long && rightValue instanceof Double) {
      accumulator = ((Long)leftValue).doubleValue()
        - (Double)rightValue;
      accumulator = (Double)accumulator*100/100;
    }
  }

  public void visitBinaryExpression(BinaryExpression binaryExpression) {
    binaryExpression.getLeftExpression().accept(this);
    binaryExpression.getRightExpression().accept(this);
  }

  @Override
  public void visit(DateValue dateValue) {
    accumulator = dateValue.getValue();
  }

  @Override
  public void visit(TimestampValue timestampValue) {}

  @Override
  public void visit(TimeValue timeValue) {}

  @Override
  public void visit(CaseExpression caseExpression) {}

  @Override
  public void visit(WhenClause whenClause) {}

  @Override
  public void visit(Concat concat) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void visit(Matches mtchs) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void visit(BitwiseAnd ba) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void visit(BitwiseOr bo) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void visit(BitwiseXor bx) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public boolean getAccumulatorBoolean() {
    return accumulatorBoolean;
  }

  public void setAccumulatorBoolean(boolean accumulatorBoolean) {
    this.accumulatorBoolean = accumulatorBoolean;
  }

  @Override
  public void visit(InExpression arg0) {}

  @Override
  public void visit(SubSelect arg0) {}

  @Override
  public void visit(AllComparisonExpression arg0) {}

  @Override
  public void visit(AnyComparisonExpression arg0) {}

  @Override
  public void visit(JdbcNamedParameter jdbcNamedParameter) {}

  @Override
  public void visit(CastExpression cast) {}

  @Override
  public void visit(Modulo modulo) {}

}
