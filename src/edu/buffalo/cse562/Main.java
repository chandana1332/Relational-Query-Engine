package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
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
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Main{
	public static void main(String args[]){
		File dataDir = null;
		File swapDir = null;
		boolean swap = false;
		LinkedList<File> sqlFiles = new LinkedList<File>();
		HashMap<String, TableInfo> tables = new HashMap<String, TableInfo>(); 
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("--data")){
				dataDir = new File(args[i+1]);
				i++;
			}
			else if(args[i].equals("--swap")){
				swapDir = new File(args[i+1]);
				i++;
				swap = false;
			}
			else{
				sqlFiles.add(new File(args[i]));
			}
		}
		
		for(File sqlFile : sqlFiles){
			try{
				FileReader stream = new FileReader(sqlFile);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement statement;
				while((statement = parser.Statement()) != null){
					//create statement
					if(statement instanceof CreateTable){
						CreateTable ct = (CreateTable)statement;
						TableInfo tableInfo = new TableInfo();
						tableInfo.setTable(ct.getTable());
						String tname = ct.getTable().getName().toLowerCase();
						List columnDef = ct.getColumnDefinitions();
						HashMap<String, Integer> tupleSchema = new HashMap<String, Integer>();
						HashMap<String, Datatype> schema = new HashMap<String, Datatype>();
						int count = 0;
						for(Object o : columnDef){
							Datatype datatype = null;
							ColumnDefinition colDef = (ColumnDefinition) o;
							String dt = colDef.getColDataType().getDataType().toLowerCase();
							if(dt.contains("int"))
								datatype = Datatype.INT;
							else if(dt.contains("decimal"))
								datatype = Datatype.DECIMAL;
							else if(dt.contains("varchar") || dt.contains("char") || dt.contains("string"))
								datatype = Datatype.STRING;
							else if(dt.contains("date"))
								datatype = Datatype.DATE;
							tupleSchema.put(colDef.getColumnName(), count);
							schema.put(colDef.getColumnName(), datatype);
							tupleSchema.put(tname + "." + colDef.getColumnName(), count++);
							schema.put(tname + "." + colDef.getColumnName(), datatype);
						}
						
						tableInfo.setOldTupleSchema(tupleSchema);
						tableInfo.setOldSchema(schema);
						tableInfo.setTupleSchema(tupleSchema);
						tableInfo.setSchema(schema);
						
						tables.put(tname, tableInfo);
					}
					//Select statements
					else if(statement instanceof Statement){
						//System.err.println("Tables: "+tables);
						PlainSelect select = (PlainSelect) ((Select)statement).getSelectBody();
						//System.err.println(select);
						
						//select
						List<SelectItem> selectItems = select.getSelectItems();
						
						//from
						FromItem fromItem = select.getFromItem();
						String from = fromItem.toString();
						String alias = fromItem.getAlias();
						
						if(alias != null){
							from = from.substring(0, from.indexOf(" AS"));
							tables.get(from).getTable().setAlias(alias);
						}
						
						LinkedList<Expression> where = splitAndClauses(select.getWhere());
						RelationOperator relationOperator = new RelationOperator();
						relationOperator.setDataDir(dataDir);
						relationOperator.setWhere(where);
						LinkedList<String> columns = getColumns(selectItems, where);
						relationOperator.setColumns(columns);
						fromItem.accept(new FromItemVisitor() {

							@Override
							public void visit(SubJoin arg0) {
	
							}
							
							@Override
							public void visit(SubSelect arg0) {
								
							}
							
							@Override
							public void visit(Table arg0) {
								relationOperator.setTableInfo(tables.get(arg0.getName().toLowerCase()));
								relationOperator.setRelation(arg0.getName());
							}
						});
						
						//joins
						List<Join> joins = select.getJoins();
						//CrossProductOperator cpOperator = null;
						//NewCrossProductOperator cpOperator = null;
						HashJoin joinOperator = null;
						
						if(joins != null){
							/*cpOperator = new CrossProductOperator();
							cpOperator.setRelationList(joins);
							cpOperator.setWhere(where);
							TableInfo info = cpOperator.join(tables, relationOperator);
							from = "cross product";
							tables.put(from, info);*/
							if(swap){
								/*cpOperator = new NewCrossProductOperator();
								cpOperator.setRelationList(joins);
								cpOperator.setWhere(where);
								cpOperator.setOutputPath(swapDir.getAbsolutePath());
								cpOperator.setInputPath(dataDir.getAbsolutePath());
								cpOperator.setChild(relationOperator);
								TableInfo info = cpOperator.join(tables);
								from = "join";
								tables.put(from, info);*/
							}
							else{
								joinOperator = new HashJoin();
								joinOperator.setRelationList(joins);
								joinOperator.setWhere(where);
								TableInfo info = joinOperator.join(tables, relationOperator);
								from = "join";
								tables.put(from, info);
							}
						}
						
						//where
						//System.err.println("selection");
						SelectOperator selectOperator = new SelectOperator();
						//System.out.println(where);
						if(where.size() > 0){
							Expression exp = createExpression(where);
							selectOperator.setWhere(exp);
							selectOperator.setTableInfo(tables.get(from));
							where = null;
						}
						
						//select
						//System.err.println("projection");
						//NewProjectOperator projectOperator = new NewProjectOperator();
						ProjectOperator projectOperator = new ProjectOperator();
						projectOperator.setTableInfo(tables.get(from));						
						projectOperator.setSelectItems(selectItems);
						
						//setting up tree
						relationOperator.setChild(null);
						relationOperator.setParent(selectOperator);
						selectOperator.setParent(projectOperator);
						/*if(cpOperator == null)
							selectOperator.setChild(relationOperator);
						else
							selectOperator.setChild(cpOperator);*/
						if(joinOperator == null /*&& cpOperator == null*/)
							selectOperator.setChild(relationOperator);
						else{
							/*if(swap)
								selectOperator.setChild(cpOperator);
							else*/
								selectOperator.setChild(joinOperator);
						}
						projectOperator.setParent(null);
						/*if(where == null || where.size() == 0){
							projectOperator.setChild(selectOperator.getChild());
						}
						else*/
							projectOperator.setChild(selectOperator);
						
						//group by
						List<Column> groupBy = select.getGroupByColumnReferences();
						projectOperator.setGroupBy(groupBy);
						
						LinkedList<String> tuple;
						LinkedList<LinkedList<String>> output = new LinkedList<LinkedList<String>>();

						//long startTime = System.nanoTime();
						while((tuple = projectOperator.getNext()) != null){
							if(tuple.size() > 0)
								output.add(tuple);
						}
						//System.out.println(output);
						//long endTime = System.nanoTime();
						//System.err.println("Took "+(endTime - startTime) + " ns");
						
						if(groupBy != null){
							LinkedList<LinkedList<String>> newOutput = new LinkedList<LinkedList<String>>();
							for(LinkedList<String> list : output){
								LinkedList<String> temp = new LinkedList<String>();
								for(String s : list){
									if(s.equals("^!$")){
										newOutput.add(temp);
										temp = new LinkedList<String>();
									}
									else
										temp.add(s);
								}
							}		
							output = newOutput;
							
							//System.err.println("group by");
							//startTime = System.nanoTime();
							GroupByOperator gbOperator = new GroupByOperator();
							gbOperator.setSelectItems(selectItems);
							gbOperator.setTableInfo(projectOperator.getTableInfo());
							gbOperator.setGroupBy(groupBy);
							output = gbOperator.output(output);
							//endTime = System.nanoTime();
							//System.err.println("Took "+(endTime - startTime) + " ns");
							
							//order by
							//System.err.println("order by");
							//startTime = System.nanoTime();
							List<OrderByElement> orderBy = select.getOrderByElements();
							if(orderBy != null){
								OrderByOperator obOperator = new OrderByOperator();
								obOperator.setOrderBy(orderBy);
								obOperator.setTableInfo(gbOperator.getTableInfo());
								output = obOperator.order(output);
							}
							//endTime = System.nanoTime();
							//System.err.println("Took "+(endTime - startTime) + " ns");
						}
						
						Limit limit = select.getLimit();
						long rowCount = -1;
						if(limit != null)
							rowCount = limit.getRowCount();
						
						display(output, rowCount);
					}
				}
			}catch(IOException e){
				e.printStackTrace();
			} 
			catch (ParseException e) {
				e.printStackTrace();
			}
			catch(SQLException e){
				e.printStackTrace();
			} /*catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} */
		}
	}
	
	public static void display(LinkedList<LinkedList<String>> output, long rowCount){
		for(LinkedList<String> tuple : output){
			if(rowCount == 0)
				break;
			int size = tuple.size();
			if(size > 0){
				for(int i=0;i<size;i++){
					String s = tuple.get(i);
					try{
						s = new DecimalFormat("0.0000").format(Double.parseDouble(s));
					}
					catch(NumberFormatException e){
						s = tuple.get(i);
					}

					if(s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\'')
						s = s.substring(1, s.length()-1);
					if(i < size-1)
						System.out.print(s+"|");
					else
						System.out.print(s);
				}
				System.out.println();
			}
			rowCount--;
		}
	}
	
	public static LinkedList<Expression> splitAndClauses(Expression e) 
	{
	  LinkedList<Expression> ret = 
	     new LinkedList<Expression>();
	  if(e instanceof AndExpression){
	    AndExpression a = (AndExpression)e;
	    ret.addAll(
	      splitAndClauses(a.getLeftExpression())
	    );
	    ret.addAll(
	      splitAndClauses(a.getRightExpression())
	    );
	  } else {
	    ret.add(e);
	  }
	  return ret;
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
	
	public static LinkedList<String> getColumns(List<SelectItem> selectItems, LinkedList<Expression> where){
		LinkedList<Column> columns = new LinkedList<Column>();
		for(SelectItem s : selectItems){
			s.accept(new SelectItemVisitor() {
				
				@Override
				public void visit(SelectExpressionItem arg0) {
					Expression e = arg0.getExpression();
					if(e instanceof Column)
						columns.add((Column) e);
					else if(e instanceof Function){
						if(((Function) e).getParameters() != null){
							Expression exp = (Expression) (((Function) e).getParameters().getExpressions().get(0));
							if(exp instanceof CaseExpression){
								CaseExpression arg = (CaseExpression) exp;
								Expression when= ((WhenClause) arg.getWhenClauses().get(0)).getWhenExpression();
								 if(when instanceof OrExpression){
									 OrExpression or = (OrExpression) when;
									 columns.add((Column)((EqualsTo)or.getLeftExpression()).getLeftExpression());
									 columns.add((Column)((EqualsTo)or.getRightExpression()).getLeftExpression());
								 }
								 else if(when instanceof AndExpression){
									 AndExpression or = (AndExpression) when;
									 columns.add((Column)((NotEqualsTo)or.getLeftExpression()).getLeftExpression());
									 columns.add((Column)((NotEqualsTo)or.getRightExpression()).getLeftExpression());
								 }
							}
							else if(exp instanceof Column)
								columns.add((Column) exp);
							else if(exp instanceof Multiplication){
								Multiplication m = (Multiplication) exp;
								Expression left, right;
								if(((left = m.getLeftExpression()) != null) && (left instanceof Column)){
									columns.add((Column) left);
								}
								right = m.getRightExpression();
								if((right != null) && (right instanceof Column)){
									columns.add((Column) right);
								}
								else{
									if(right instanceof Parenthesis){
										Parenthesis p = (Parenthesis) right;
										BinaryExpression bex = (BinaryExpression) p.getExpression();
										if(((left = bex.getLeftExpression()) != null) && (left instanceof Column)){
											columns.add((Column) left);
										}
										right = bex.getRightExpression();
										if((right != null) && (right instanceof Column)){
											columns.add((Column) right);
										}
									}
									else{
										BinaryExpression bx = (BinaryExpression) right;
										if(((left = bx.getLeftExpression()) != null) && (left instanceof Parenthesis)){
											Parenthesis p = (Parenthesis) left;
											BinaryExpression bex = (BinaryExpression) p.getExpression();
											if(((left = bex.getLeftExpression()) != null) && (left instanceof Column)){
												columns.add((Column) left);
											}
											right = bex.getRightExpression();
											if((right != null) && (right instanceof Column)){
												columns.add((Column) right);
											}
										}
										right = bx.getRightExpression();
										if((right != null) && (right instanceof Parenthesis)){
											Parenthesis p = (Parenthesis) right;
											BinaryExpression bex = (BinaryExpression) p.getExpression();
											if(((left = bex.getLeftExpression()) != null) && (left instanceof Column)){
												columns.add((Column) left);
											}
											right = bex.getRightExpression();
											if((right != null) && (right instanceof Column)){
												columns.add((Column) right);
											}
											columns.add((Column) left);
										}
									}
								}
							}
						}
					}
				}
				
				@Override
				public void visit(AllTableColumns arg0) {
					
				}
				
				@Override
				public void visit(AllColumns arg0) {
					
				}
			});
		}
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
					 Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
				}
				
				@Override
				public void visit(MinorThan arg0) {
					Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
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
					Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
				}
				
				@Override
				public void visit(GreaterThan arg0) {
					Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
				}
				
				@Override
				public void visit(EqualsTo arg0) {
					Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
				}
				
				@Override
				public void visit(Between arg0) {
					 
					
				}
				
				@Override
				public void visit(OrExpression arg0) {
					Expression left, right = null;
					 if((left = arg0.getLeftExpression()) != null && (left instanceof Column))
						columns.add((Column) left);
					 if((right = arg0.getRightExpression()) != null && (right instanceof Column))
							columns.add((Column) right);										
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
		
		Set<String> colNames = new HashSet<String>();
		LinkedList<String> finalCol = new LinkedList<String>();
		for(Column c : columns){
			if(!(colNames.contains(c.getColumnName()))){
				String colname = c.getColumnName();
				finalCol.add(colname.substring(colname.indexOf('.')+1, colname.length()));
				colNames.add(colname);
			}
				
		}
		
		return finalCol;
	}
}
