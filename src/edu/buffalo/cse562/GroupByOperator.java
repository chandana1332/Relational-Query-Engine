package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class GroupByOperator extends Eval implements Operator {

	private TableInfo tableInfo;
	private LinkedList<String> tuple;
	private Operator parent;
	private Operator child;
	private List<SelectItem> selectItems;
	private List<Column> groupBy;
	
	public LinkedList<LinkedList<String>> output(LinkedList<LinkedList<String>> o){
		HashMap<String, Integer> tupleSchema = new HashMap<String, Integer>();
		HashMap<String, Datatype> schema = new HashMap<String, Datatype>();	
		int count = 0;
		for(Column c : groupBy){
			tupleSchema.put(c.getWholeColumnName(), count++);
			Datatype cd = this.tableInfo.getOldSchema().get(c.getColumnName());
			if(cd == null)
				cd = this.tableInfo.getSchema().get(c.getWholeColumnName());
			schema.put(c.getWholeColumnName(), cd);
		}
		
		for(SelectItem item : selectItems){
			if(item instanceof SelectExpressionItem){
				SelectExpressionItem seItem = (SelectExpressionItem) item;
				if(seItem.getExpression() instanceof Function){
					if(seItem.getAlias() == null){
						Function fn = (Function) seItem.getExpression();
						tupleSchema.put(fn.toString(), count++);
						schema.put(fn.toString(), Datatype.DECIMAL);
					}
					else{
						tupleSchema.put(seItem.getAlias(), count++);
						schema.put(seItem.getAlias(), Datatype.DECIMAL);
					}
					
				}
			}
		}
		this.tableInfo.setSchema(schema);
		this.tableInfo.setTupleSchema(tupleSchema);
		
		LinkedList<LinkedList<String>> output = new LinkedList<LinkedList<String>>();
		for(LinkedList<String> tempTuple : o){
			this.setTuple(tempTuple);
			LinkedList<String> temp = new LinkedList<String>();
			
			for(SelectItem item : selectItems){
				item.accept(new SelectItemVisitor() {
					
					@Override
					public void visit(SelectExpressionItem arg0) {
						// TODO Auto-generated method stub
						if(arg0.getExpression() instanceof Function){
							String s;
							if(arg0.getAlias() == null)
								s = arg0.getExpression().toString();
							else
								s = arg0.getAlias();
							int colID = tupleSchema.get(s);
							temp.add(getTuple().get(colID));
						}
						else{
							if(tempTuple != null && tempTuple.size() !=0){
								try {
									temp.add(eval(arg0.getExpression()).toString());
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
					}
					
					@Override
					public void visit(AllTableColumns arg0) {
						// TODO Auto-generated method stub
						
					}
					
					@Override
					public void visit(AllColumns arg0) {
						// TODO Auto-generated method stub
						
					}
				});
			}
			output.add(temp);
		}
		
		HashMap<String, Integer> tupleSchema1 = new HashMap<String, Integer>();
		HashMap<String, Datatype> schema1 = new HashMap<String, Datatype>();	
		count = 0;
		for(SelectItem item : selectItems){
			item.accept(new SelectItemVisitor() {
				
				@Override
				public void visit(SelectExpressionItem arg0) {
					Datatype colDef;
					if(arg0.getExpression() instanceof Function){
						if(arg0.getAlias() == null){
							colDef = schema.get(arg0.getExpression().toString());
							tupleSchema1.put(arg0.getExpression().toString(), tupleSchema1.size());
							schema1.put(arg0.getExpression().toString(), colDef);
						}
						else{
							colDef = schema.get(arg0.getAlias());
							tupleSchema1.put(arg0.getAlias(), tupleSchema1.size());
							schema1.put(arg0.getAlias(), colDef);
						}
					}
					else{
						Expression e = arg0.getExpression();
						if(e instanceof Column){
							String colName = ((Column) e).getColumnName();
							colDef = schema.get(arg0.getExpression().toString());
							tupleSchema1.put(colName, tupleSchema1.size());
							schema1.put(colName, colDef);
						}
						else{

							colDef = schema.get(arg0.getExpression().toString());
							tupleSchema1.put(arg0.getExpression().toString(), tupleSchema1.size());
							schema1.put(arg0.getExpression().toString(), colDef);
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
			this.tableInfo.setSchema(schema1);
			this.tableInfo.setTupleSchema(tupleSchema1);
		}
		
		//System.out.println(output);
		return output;
	}
	
	public List<SelectItem> getSelectItems() {
		return selectItems;
	}

	public void setSelectItems(List<SelectItem> selectItems) {
		this.selectItems = selectItems;
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
				value = new StringValue(tuple.get(colID));
				break;
			case DATE:
				value = new DateValue(" " + tuple.get(colID) + " ");
				break;
		}
		return value;
	}

	@Override
	public LinkedList<String> getNext() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public List<Column> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Column> groupBy) {
		this.groupBy = groupBy;
	}
}
