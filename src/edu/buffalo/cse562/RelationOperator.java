package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SubSelect;

public class RelationOperator extends Eval implements Operator {
	private String relation;
	private LinkedList<String> tuple;
	private File dataDir;
	private boolean join;
	private FileReader tableStream;
	private BufferedReader br;
	private Operator child;
	private Operator parent;
	private LinkedList<Expression> where;
	private TableInfo tableInfo;
	private LinkedList<Expression> expressions;
	private String swapDir;
	private LinkedList<String> columns; 
	private Set<Integer> colID = new HashSet<Integer>();
	
	public LinkedList<String> getColumns() {
		return columns;
	}

	public void setColumns(LinkedList<String> columns) {
		this.columns = columns;
	}

	public LinkedList<Expression> getWhere() {
		return where;
	}

	public void setWhere(LinkedList<Expression> where) {
		this.where = where;
	}

	public String getRelation() {
		return relation;
	}

	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	public void setRelation(String relation) {
		this.relation = relation;
		tuple = new LinkedList<String>();
		try {
			
			if(relation.equalsIgnoreCase("join"))
			{
				join=true;
				//System.out.println(swapDir + File.separator + this.relation + ".dat");
				tableStream = new FileReader(swapDir + File.separator + this.relation + ".dat");
			}
			else
			{
				tableStream = new FileReader(dataDir.getAbsolutePath() + File.separator + this.relation + ".dat");
			}

			br = new BufferedReader(tableStream);
			expressions = new LinkedList<Expression>();
			for(Expression e : where){
				e.accept(new ExpressionVisitor() {
					
					@Override
					public void visit(BitwiseXor arg0) {
						 
						
					}
					
					@Override
					public void visit(BitwiseOr arg0) {
						 
						
					}
					
					@Override
					public void visit(BitwiseAnd arg0) {
						 
						
					}
					
					@Override
					public void visit(Matches arg0) {
						 
						
					}
					
					@Override
					public void visit(Concat arg0) {
						 
						
					}
					
					@Override
					public void visit(AnyComparisonExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(AllComparisonExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(ExistsExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(WhenClause arg0) {
						 
						
					}
					
					@Override
					public void visit(CaseExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(SubSelect arg0) {
						 
						
					}
					
					@Override
					public void visit(Column arg0) {
						 
						
					}
					
					@Override
					public void visit(NotEqualsTo arg0) {
						 
						
					}
					
					@Override
					public void visit(MinorThanEquals arg0) {
						Expression left = arg0.getLeftExpression();
						if((left instanceof Column) && !(arg0.getRightExpression() instanceof Column)){
							if(tableInfo.getTupleSchema().containsKey(left.toString())){
								expressions.add(arg0);
							}
						}						
					}
					
					@Override
					public void visit(MinorThan arg0) {
						Expression left = arg0.getLeftExpression();
						if((left instanceof Column) && !(arg0.getRightExpression() instanceof Column)){
							if(tableInfo.getTupleSchema().containsKey(left.toString())){
								expressions.add(arg0);
							}
						}	
					}
					
					@Override
					public void visit(LikeExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(IsNullExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(InExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(GreaterThanEquals arg0) {
						Expression left = arg0.getLeftExpression();
						if((left instanceof Column) && !(arg0.getRightExpression() instanceof Column)){
							if(tableInfo.getTupleSchema().containsKey(left.toString())){
								expressions.add(arg0);
							}
						}	
					}
					
					@Override
					public void visit(GreaterThan arg0) {
						//System.out.println(arg0);
						GreaterThan exp = (GreaterThan) arg0;
						Expression left = exp.getLeftExpression();
						if((left instanceof Column) && !(exp.getRightExpression() instanceof Column)){
							if(tableInfo.getTupleSchema().containsKey(left.toString())){
								expressions.add(exp);
							}
						}	
					}
					
					@Override
					public void visit(EqualsTo arg0) {
						Expression left = arg0.getLeftExpression();
						if((left instanceof Column) && !(arg0.getRightExpression() instanceof Column)){
							if(tableInfo.getTupleSchema().containsKey(left.toString())){
								expressions.add(arg0);
							}
						}	
					}
					
					@Override
					public void visit(Between arg0) {
						 
						
					}
					
					@Override
					public void visit(OrExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(AndExpression arg0) {
						 
						
					}
					
					@Override
					public void visit(Subtraction arg0) {
						 
						
					}
					
					@Override
					public void visit(Multiplication arg0) {
						 
						
					}
					
					@Override
					public void visit(Division arg0) {
						 
						
					}
					
					@Override
					public void visit(Addition arg0) {
						 
						
					}
					
					@Override
					public void visit(StringValue arg0) {
						 
						
					}
					
					@Override
					public void visit(Parenthesis arg0) {
						 
						
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
			where.removeAll(expressions);
			
			if(columns != null){				
				newSchema();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public LinkedList<String> getNext()throws SQLException{
		try {
			if(br != null){
				String tableRow = br.readLine();
				if(tableRow != null){
					String splitArray[] = tableRow.split("\\|");
					if(tuple == null)
						return null;
					tuple = new LinkedList<String>();
					for(String temp : splitArray){
						if(!(temp.equals("|"))){
							tuple.add(temp);
						}
					}
					if(columns != null){
						LinkedList<String> temptuple = new LinkedList<String>();
						for(int c : colID){
							temptuple.add(tuple.get(c));
						}
						tuple = temptuple;
					}
					/*else{
						LinkedList<String> temptuple = new LinkedList<String>();
						int colIDCount = 0;
						for(String temp : splitArray){
							if(!(temp.equals("|"))){
								if(colID.contains(colIDCount))
									tuple.add(temp);
								colIDCount++;
							}
						}
					}*/
					if(expressions.size() > 0){
						Expression e = createExpression(expressions);
						
						e.accept(new ExpressionVisitor() {
							
							@Override
							public void visit(BitwiseXor arg0) {
								 
								
							}
							
							@Override
							public void visit(BitwiseOr arg0) {
								 
								
							}
							
							@Override
							public void visit(BitwiseAnd arg0) {
								 
								
							}
							
							@Override
							public void visit(Matches arg0) {
								 
								
							}
							
							@Override
							public void visit(Concat arg0) {
								 
								
							}
							
							@Override
							public void visit(AnyComparisonExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(AllComparisonExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(ExistsExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(WhenClause arg0) {
								 
								
							}
							
							@Override
							public void visit(CaseExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(SubSelect arg0) {
								 
								
							}
							
							@Override
							public void visit(Column arg0) {
								 
								
							}
							
							@Override
							public void visit(NotEqualsTo arg0) {
								 
								
							}
							
							@Override
							public void visit(MinorThanEquals arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
							
							@Override
							public void visit(MinorThan arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}								
							}
							
							@Override
							public void visit(LikeExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(IsNullExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(InExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(GreaterThanEquals arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}								
							}
							
							@Override
							public void visit(GreaterThan arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}								
							}
							
							@Override
							public void visit(EqualsTo arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}		
								
							}
							
							@Override
							public void visit(Between arg0) {
								 
								
							}
							
							@Override
							public void visit(OrExpression arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}								
							}
							
							@Override
							public void visit(AndExpression arg0) {
								 
								
							}
							
							@Override
							public void visit(Subtraction arg0) {
								 
								
							}
							
							@Override
							public void visit(Multiplication arg0) {
								 
								
							}
							
							@Override
							public void visit(Division arg0) {
								 
								
							}
							
							@Override
							public void visit(Addition arg0) {
								 
								
							}
							
							@Override
							public void visit(StringValue arg0) {
								 
								
							}
							
							@Override
							public void visit(Parenthesis arg0) {
								try {
									if(!((BooleanValue)eval(arg0)).getValue()){
										tuple.clear();
									}
								} catch (SQLException e) {
									e.printStackTrace();
								}								
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
					if(tuple.size() == 0)
						tuple = getNext();
					//System.out.println(tuple);
					return tuple;
				}
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
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
	
	public static Expression createExpression(LinkedList<Expression> where){
		int size = where.size();
		while(size > 1){
			LinkedList<Expression> expList = new LinkedList<Expression>();
			if(size%2 == 0){
				for(int i=0;i<size;i=i+2){
					AndExpression a = new AndExpression(where.get(i), where.get(i+1));
					expList.add(a);
				}
			}
			else{
				for(int i=0;i<size-1;i=i+2){
					AndExpression a = new AndExpression(where.get(i), where.get(i+1));
					expList.add(a);
				}
				expList.add(where.get(size-1));
			}
			where = expList;
			size = where.size();
		}
		return where.get(0);
	}

	public File getDataDir() {
		return dataDir;
	}

	public void setDataDir(File dataDir) {
		this.dataDir = dataDir;
	}
	public void setswapDir(String a) {
		this.swapDir = a;
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
	
	public void newSchema(){
		colID.clear();
		MyComparator comp = new MyComparator(tableInfo.getOldTupleSchema());
	    Map<String,Integer> tupleSchema = new TreeMap(comp);
	    tupleSchema.putAll(tableInfo.getOldTupleSchema());
		
		HashMap<String, Datatype> schema = tableInfo.getOldSchema();
		HashMap<String, Integer> newTupleSchema = new HashMap<String, Integer>();
		HashMap<String, Datatype> newSchema = new HashMap<String, Datatype>();
		int count = 0;
		for( Entry<String, Integer> entry : tupleSchema.entrySet()){
			String key = entry.getKey();
			String colName = key.substring(key.indexOf('.')+1, key.length());
			if(columns.contains(colName)){
				int colID = tableInfo.getOldTupleSchema().get(colName);
				this.colID.add(colID);
				newTupleSchema.put(this.relation+"."+colName, count);
				newTupleSchema.put(colName, count++);
				newSchema.put(colName, schema.get(colName));
				newSchema.put(this.relation+"."+colName, schema.get(colName));
			}
		}
		
		tableInfo.setTupleSchema(newTupleSchema);
		tableInfo.setSchema(newSchema);
		tableInfo.setFirst(false);
	}
	
}

class MyComparator implements Comparator {

	HashMap map;
	
	public MyComparator(HashMap map) {
	    this.map = map;
	}
	
	public int compare(Object o1, Object o2) {
	
	    return ((Integer) map.get(o1)).compareTo((Integer) map.get(o2));
	
	}
}