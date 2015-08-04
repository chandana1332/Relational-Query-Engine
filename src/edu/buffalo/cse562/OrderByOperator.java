package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

public class OrderByOperator implements Operator {

	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private List<OrderByElement> orderBy;
	private LinkedList<String> tuple;

	public LinkedList<LinkedList<String>> order(LinkedList<LinkedList<String>> o){
		Stack<OrderByElement> stack = new Stack<OrderByElement>();
		for(OrderByElement element : orderBy){
			stack.push(element);
		}
		while(!stack.empty()){
			OrderByElement element = stack.pop();
			//System.out.println(element);
			element.accept(new OrderByVisitor() {
				
				@Override
				public void visit(OrderByElement arg0) {
					// TODO Auto-generated method stub
					//System.out.println(tableInfo.getTupleSchema());
					//System.out.println(arg0);
					int colID = tableInfo.getTupleSchema().get(arg0.getExpression().toString());
					boolean isAsc = arg0.isAsc();
					Datatype dataType = tableInfo.getSchema().get(arg0.getExpression().toString());
					//System.out.println(colID+" "+dataType);
					Collections.sort(o, new Comparator<LinkedList<String>>() {
					    @Override
					    public int compare(LinkedList<String> one, LinkedList<String> two) {
					    	
							switch(dataType){
								case INT:
									Long value1 = new Long(one.get(colID));
						    		Long value2 = new Long(two.get(colID));
						    		if(isAsc)
						    			return value1.compareTo(value2);
						    		else
						    			return value2.compareTo(value1);
								case DECIMAL:
									Double dvalue1 = new Double(one.get(colID));
									Double dvalue2 = new Double(two.get(colID));
									if(isAsc)
						    			return dvalue1.compareTo(dvalue2);
						    		else
						    			return dvalue2.compareTo(dvalue1);
								case STRING:
									if(isAsc)
										return one.get(colID).compareTo(two.get(colID));
									else
										return two.get(colID).compareTo(one.get(colID));
								case DATE:
									DateValue dv1 = new DateValue(" "+one.get(colID)+" ");
									DateValue dv2 = new DateValue(" "+two.get(colID)+" ");
									if(isAsc)
										return dv1.getValue().compareTo(dv2.getValue());
									else
										return dv2.getValue().compareTo(dv1.getValue());
							}
					    	
					    	return 0;
					    }
					});
				}
			});
		}
		
		return o;
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

	@Override
	public LinkedList<String> getNext() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<OrderByElement> getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(List<OrderByElement> orderBy) {
		this.orderBy = orderBy;
	}
	
	public LinkedList<String> getTuple() {
		return tuple;
	}

	public void setTuple(LinkedList<String> tuple) {
		this.tuple = tuple;
	}


}
