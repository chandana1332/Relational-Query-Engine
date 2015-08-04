package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectOperator extends Eval implements Operator {
	
	private Expression where;
	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private LinkedList<String> tuple;
	
	public LinkedList<String> getNext()throws SQLException{
		LinkedList<String> tempTuple = null;
		if(this.getChild() instanceof RelationOperator)
			tempTuple = ((RelationOperator)this.getChild()).getNext();
		else{
			if(this.getChild() instanceof HashJoin)
				tempTuple = ((HashJoin)this.getChild()).getNext();
			//else
				//tempTuple = ((NewCrossProductOperator)this.getChild()).getNext();
		}
		if(tempTuple != null){
			this.setTuple(tempTuple);
			//System.out.println(where);
			if(where != null){
				where.accept(new ExpressionVisitor() {
					
					@Override
					public void visit(BitwiseXor arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					
					@Override
					public void visit(BitwiseOr arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(BitwiseAnd arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Matches arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					
					@Override
					public void visit(Concat arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(AnyComparisonExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(AllComparisonExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(ExistsExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(WhenClause arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(CaseExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(SubSelect arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Column arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(NotEqualsTo arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(MinorThanEquals arg0) {
						try {
							if(eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(MinorThan arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(LikeExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(IsNullExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(InExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(GreaterThanEquals arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(GreaterThan arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(EqualsTo arg0){
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					
					@Override
					public void visit(Between arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(OrExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(AndExpression arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Subtraction arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Multiplication arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Division arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Addition arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(Parenthesis arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					
					@Override
					public void visit(StringValue arg0) {
					
					}
					
					@Override
					public void visit(TimestampValue arg0) {
						 
						
					}
					
					@Override
					public void visit(TimeValue arg0) {
						 
						
					}
					
					@Override
					public void visit(DateValue arg0) {
						 
						
					}
					
					@Override
					public void visit(LongValue arg0) {
						 
						
					}
					
					@Override
					public void visit(DoubleValue arg0) {
						 
						
					}
					
					@Override
					public void visit(JdbcParameter arg0) {
						try {
							if(!((BooleanValue)eval(arg0)).getValue())
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}						
					}
					
					@Override
					public void visit(InverseExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(Function arg0) {
											
					}
					
					@Override
					public void visit(NullValue arg0) {
						 
						
					}
				});
			}
			//System.out.println(tuple);
			return tuple;
		}
		return null;
	}

	public Expression getWhere() {
		return where;
	}

	public void setWhere(Expression where) {
		this.where = where;
	}
	
	public Operator getParent() {
		return parent;
	}
	
	public void setParent(Operator parent) {
		this.parent = parent;
	}
	
	public Operator getChild() {
		return child;
	}
	
	public void setChild(Operator child) {
		this.child = child;
	}
	
	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	public LinkedList<String> getTuple() {
		return tuple;
	}

	public void setTuple(LinkedList<String> tuple) {
		this.tuple = tuple;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		//System.out.println(tuple);
		//System.err.println("Select: "+tableInfo);
		//System.out.println("Select: "+tableInfo.getTupleSchema());
		Integer colID = this.tableInfo.getTupleSchema().get(arg0.getWholeColumnName());
		Datatype dataType = this.tableInfo.getSchema().get(arg0.getWholeColumnName());
		
		LeafValue value = null;
		switch(dataType){
			case INT:
				value = new LongValue(tuple.get(colID));
				break;
			case DECIMAL:
				value = new DoubleValue(tuple.get(colID));
				break;
			case STRING:
				value = new StringValue("'" + tuple.get(colID) + "'");
				break;
			case DATE:
				value = new DateValue(" " + tuple.get(colID) + " ");
				break;
		}
		return value;
	}
}
