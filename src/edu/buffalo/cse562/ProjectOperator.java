package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class ProjectOperator extends Eval implements Operator{
	
	private List<SelectItem> selectItems;
	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private LinkedList<String> tuple;
	private List<Column> groupBy;
	private LinkedHashMap<String, HashMap<LinkedList<String>, LinkedList<LeafValue>>> group1 = new LinkedHashMap<String, HashMap<LinkedList<String>,LinkedList<LeafValue>>>();
	private boolean flag = true;
	
	public LinkedList<String> getNext()throws SQLException{
		LinkedList<String> tempTuple= this.getChild().getNext();
		LinkedList<String> tuple = new LinkedList<String>();
		if((tempTuple != null && tempTuple.size() !=0) || flag){
			this.setTuple(tempTuple);
			for(SelectItem item : selectItems){
				item.accept(new SelectItemVisitor() {
					@Override
					public void visit(SelectExpressionItem arg0) {
						try {
							if(arg0.getExpression() instanceof Function){
								Function fn = (Function)arg0.getExpression();
								String function = fn.toString();
								String fnName = fn.getName();
								if(!group1.containsKey(function))
									group1.put(function, new HashMap<LinkedList<String>, LinkedList<LeafValue>>());
								ExpressionList parameters = fn.getParameters();
								if(parameters == null){				
									if(tempTuple == null){
										if(flag){
											if(groupBy == null){
												for(HashMap<LinkedList<String>, LinkedList<LeafValue>> entry1 : group1.values()){
													for(LinkedList<LeafValue> entry : entry1.values()){
														tuple.add(entry.get(0).toString());
													}
												}
											}
											else{
												boolean first = true;
												LinkedList<String> key = null;
												for(HashMap<LinkedList<String>, LinkedList<LeafValue>> entry1 : group1.values()){
													for(Entry<LinkedList<String>, LinkedList<LeafValue>> entry : entry1.entrySet()){
														if(first){
															key = entry.getKey();
															tuple.addAll(key);
															first = false;
														}
														if(entry.getKey() == key)
															tuple.add(entry.getValue().get(0).toString());
													}
													first = true;
													tuple.add("^!$");
												}
											}
										}
										flag=false;
									}
									else if(tempTuple.size() != 0){
										if(function.contains("count(")){
											if(groupBy == null){
												count(function, null);
											}
											else{
												LinkedList<String> groupByItems = new LinkedList<String>();
												for(Column c : groupBy){
													groupByItems.add(eval(c).toString());
												}
												count(function, groupByItems);
												//count(eval(groupBy.get(0)).toString());
											}
										}
									}
								}
								else{
									Expression e = (Expression) parameters.getExpressions().get(0);
									if(tempTuple == null){
										if(flag){
											if(groupBy == null){
												for(Entry<String, HashMap<LinkedList<String>, LinkedList<LeafValue>>> entry1 : group1.entrySet()){
													HashMap<LinkedList<String>, LinkedList<LeafValue>> group = entry1.getValue();
													if(entry1.getKey().contains("count(") && !entry1.getKey().contains("*")){
														for(LinkedList<LeafValue> entry : group.values()){
															tuple.add(entry.size()+"");
														}
													}
													else if(entry1.getKey().contains("avg")){
														for(LinkedList<LeafValue> entry : group.values()){
															tuple.add(entry.get(2).toString());
														}
													}
													else{
														for(LinkedList<LeafValue> entry : group.values()){
															tuple.add(entry.get(0).toString());
														}
													}
												}
											}
											else{
												Set<LinkedList<String>> keySet = null;
												for(Entry<String, HashMap<LinkedList<String>, LinkedList<LeafValue>>> entry1 : group1.entrySet()){
													HashMap<LinkedList<String>, LinkedList<LeafValue>> group = entry1.getValue();
													keySet = group.keySet();
													break;
												}
												for(LinkedList<String> key : keySet){
													tuple.addAll(key);
													for(Entry<String, HashMap<LinkedList<String>, LinkedList<LeafValue>>> entry1 : group1.entrySet()){
														HashMap<LinkedList<String>, LinkedList<LeafValue>> group = entry1.getValue();
														//if(entry1.getKey().contains("count("))
														//	tuple.add(group.get(key).size()+"");
														//else
														if(entry1.getKey().contains("avg")){
															tuple.add(group.get(key).get(2).toString());
														}
														else
															tuple.add(group.get(key).get(0).toString());
													}

													tuple.add("^!$");
												}													
											}
										}
										flag=false;
									}
									else if(tempTuple.size() != 0){
										LinkedList<String> cValue = new LinkedList<String>();
										if(groupBy == null){
											cValue.add(e.toString());
											HashMap<LinkedList<String>, LinkedList<LeafValue>> group = group1.get(function);
											if(group.containsKey(cValue)){
												LinkedList<LeafValue> l = group.get(cValue);
												l.add(eval(e));
												group.put(cValue, l);
											}
											else{
												LinkedList<LeafValue> l = new LinkedList<LeafValue>();
												l.add(eval(e));
												group.put(cValue, l);
											}
										}
										else{
											for(Column c : groupBy){
												cValue.add(eval(c).toString());
											}
											HashMap<LinkedList<String>, LinkedList<LeafValue>> group = group1.get(function);
											if(group.containsKey(cValue)){
												LinkedList<LeafValue> l = group.get(cValue);
												l.add(eval(e));
												group.put(cValue, l);
											}
											else{
												LinkedList<LeafValue> l = new LinkedList<LeafValue>();
												l.add(eval(e));
												group.put(cValue, l);
											}
										}
										
										if(fnName.equalsIgnoreCase("SUM")){
											sum(function, cValue);
										}
										else if(fnName.equalsIgnoreCase("AVG")){
											avg(function, cValue);
										}
									}
								}
							}
							else if(!(arg0.getExpression() instanceof Function) && groupBy == null){
								flag = false;
								if(tempTuple != null && tempTuple.size() !=0)
									tuple.add(eval(arg0.getExpression()).toString());
							}
							
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
					
					@Override
					public void visit(AllTableColumns arg0) {
						
					}
					
					@Override
					public void visit(AllColumns arg0) {
						flag = false;
						if(tempTuple != null && tempTuple.size() != 0)
							tuple.addAll(tempTuple);
					}
				});
			}
			//System.out.println(group1);
			//System.out.println(tuple);
			return tuple;
		}

		if(tempTuple == null)
			return null;
		return tuple;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		//System.out.println(this.tableInfo.getTupleSchema());
		//System.out.println(arg0);
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
	
	public void sum(String fnName, LinkedList<String> columnName){
		try{
			HashMap<LinkedList<String>, LinkedList<LeafValue>> group = group1.get(fnName);
			LinkedList<LeafValue> l = group.get(columnName);
			LeafValue sum = l.get(0);
			if(l.size() > 1)
				sum = new DoubleValue(l.get(1).toDouble() + sum.toDouble());
			l.clear();
			l.add(sum);
			group.put(columnName, l);
		}
		catch(InvalidLeaf e){
			e.printStackTrace();
		}
	}
	
	public void avg(String fnName, LinkedList<String> columnName){
		try{
			HashMap<LinkedList<String>, LinkedList<LeafValue>> group = group1.get(fnName);
			LinkedList<LeafValue> l = group.get(columnName);
			long count = 0;
			if(l.size() > 1)
				count = l.get(1).toLong();
			LeafValue sum = new DoubleValue(l.get(0).toDouble());
			if(l.size() == 4){
				sum = new DoubleValue(l.get(3).toDouble() + sum.toDouble());
			}
			count++;
			//System.out.println(count);
			LeafValue avg = new DoubleValue(sum.toDouble()/count);
			l.clear();
			l.add(sum);
			l.add(new LongValue(count));
			l.add(avg);
			group.put(columnName, l);
		}
		catch(InvalidLeaf e){
			e.printStackTrace();
		}
	}
	
	public void count(String fnName, LinkedList<String> columnName){
		HashMap<LinkedList<String>, LinkedList<LeafValue>> group = group1.get(fnName);
		if(columnName == null){
			columnName = new LinkedList<String>();
			columnName.add("*");
		}
		try{
			if(group.containsKey(columnName)){
				LinkedList<LeafValue> lv = group.get(columnName);
				LongValue l = new LongValue(lv.get(0).toLong()+1);
				lv.clear();
				lv.add(l);
				group.put(columnName, lv);
			}
			else{
				LinkedList<LeafValue> lv = new LinkedList<LeafValue>();
				LongValue l = new LongValue(1);
				lv.add(l);
				group.put(columnName, lv);
			}
		}
		catch(InvalidLeaf e){
			e.printStackTrace();
		}
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

	public List<Column> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Column> groupBy) {
		this.groupBy = groupBy;
	}

}
