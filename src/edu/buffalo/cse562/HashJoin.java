

package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;

public class HashJoin extends Eval implements Operator{
	private List<Join> relationList;
	private LinkedList<LinkedList<String>> cpTable = new LinkedList<LinkedList<String>>();
	private Stack<LinkedList<String>> joinTable = new Stack<LinkedList<String>>();
	private Operator child;
	private Operator parent;
	private TableInfo cpTableInfo;
	private LinkedList<String> tuple;
	private LinkedList<Expression> where;
	private LinkedList<String> tableList;
	
	public TableInfo join(HashMap<String, TableInfo> tables, RelationOperator relationOperator)throws SQLException{
		tableList = new LinkedList<String>();
		LinkedList<String> tuple;
		String tname1, tname2;
		int t1=0, t2=0;
		String tableName = relationOperator.getRelation();
		tname1 = tableName;
		cpTableInfo = new TableInfo();
		cpTableInfo.getTable().setName("join");
		newSchema(cpTableInfo, tables.get(tableName));
		tables.put("join", cpTableInfo);
		
		while((tuple = relationOperator.getNext()) != null){
			cpTable.add(tuple);
		}
		//System.out.println(tableName+" "+cpTable);
		
		LinkedList<Expression> expList = new LinkedList<Expression>();
		//System.out.println(relationList);
		for(Join listItem : relationList){
			tableName = listItem.getRightItem().toString();
			tname2 = tableName;
			String alias = listItem.getRightItem().getAlias();
			if(alias != null){
				tableName = tableName.substring(0, tableName.indexOf(" AS"));
				tname2 = tableName;
				tables.get(tableName).getTable().setAlias(alias);
			}
			//System.out.println(tname1+" "+tname2);
			Expression on = listItem.getOnExpression();
			if(on == null){
				for(Expression e : where){
					if(e instanceof EqualsTo){
						EqualsTo eq = (EqualsTo) e;
						Expression left = eq.getLeftExpression();
						Expression right = eq.getRightExpression();
						if((left instanceof Column) && (right instanceof Column)){
							Column leftCol = (Column) left;
							Column rightCol = (Column) right;
							//System.out.println(tables.get(tname1).getTupleSchema());
							//System.out.println(tables.get(tname2).getTupleSchema());
							//System.out.println(leftCol+" "+rightCol);
							if((tables.get(tname1).getTupleSchema().containsKey(leftCol.getWholeColumnName())
									|| tables.get(tname2).getTupleSchema().containsKey(leftCol.getWholeColumnName())) && 
									(tables.get(tname1).getTupleSchema().containsKey(rightCol.getWholeColumnName())
											|| tables.get(tname2).getTupleSchema().containsKey(rightCol.getWholeColumnName()))){
								if(!expList.contains(e))
									on = e;
								expList.add(e);
							}
						}
					}
					else if(e instanceof Parenthesis){
						Parenthesis p = (Parenthesis) e;
						Expression exp = p.getExpression();
						if(exp instanceof EqualsTo){
							EqualsTo eq = (EqualsTo) exp;
							Expression left = eq.getLeftExpression();
							Expression right = eq.getRightExpression();
							if((left instanceof Column) && (right instanceof Column)){
								Column leftCol = (Column) left;
								Column rightCol = (Column) right;
								if((tables.get(tname1).getTupleSchema().containsKey(leftCol.getColumnName()) 
										&& tables.get(tname2).getTupleSchema().containsKey(rightCol.getColumnName())) ||
										(tables.get(tname2).getTupleSchema().containsKey(leftCol.getColumnName()) 
												&& tables.get(tname1).getTupleSchema().containsKey(rightCol.getColumnName()))){
									if(!expList.contains(exp))
										on = exp;
									expList.add(p);
								}
							}
						}
					}
				}
			}	
			//System.out.println(on);
			relationOperator.setTableInfo(tables.get(tableName));
			relationOperator.setRelation(tableName);
			newSchema(cpTableInfo, tables.get(tableName));
			//System.out.println(cpTableInfo.getTupleSchema());
			if(on!=null)
			{
				String condition=on.toString();
				
				String c[]=condition.split(" = ");
				
				String l[]=c[0].split("[.]");
				String r[]=c[1].split("[.]");
				
				boolean flag = true;
				if(tableList.contains(l[0])){
					l[0] = "join";
					l[1] = c[0];
					
					HashMap<String, Integer> tup1=tables.get("join").getTupleSchema();
					//System.out.println("join"+tup1);
					t1=tup1.get(l[1]);
					flag = false;
					
				}
				else if(tableList.contains(r[0])){
					r[0] = "join";
					r[1] = c[1];
					HashMap<String, Integer> tup1=tables.get("join").getTupleSchema();
					//System.out.println("join"+tup1);
					t1=tup1.get(r[1]);
					flag = false;
				}
				
				if(l[0].equalsIgnoreCase(tname1) && flag)
				{
					 HashMap<String, Integer> tup1=tables.get(tname1).getTupleSchema();
					 //System.out.println(tname1+tup1);
					 t1=tup1.get(l[1]);
				}				
				else if(l[0].equalsIgnoreCase(tname2))
				{
					 HashMap<String, Integer> tup2=tables.get(tname2).getTupleSchema();
					 //System.out.println(tname2+tup2);
					 t2=tup2.get(l[1]);
				} 
				
				//System.out.println(r[0]+" "+r[1]+" ");
				if(r[0].equalsIgnoreCase(tname1) && flag)
				{
					 HashMap<String, Integer> tup1=tables.get(tname1).getTupleSchema();
					 //System.out.println(tname1+tup1);
					 t1=tup1.get(r[1]);
				}				
				else if(r[0].equalsIgnoreCase(tname2))
				{
					 HashMap<String, Integer> tup2=tables.get(tname2).getTupleSchema();
					 //System.out.println(tname2+tup2);
					 t2=tup2.get(r[1]);
				} 
				
			}
			relationOperator.setTableInfo(tables.get(tname2));
			relationOperator.setRelation(tname2);
			//System.out.println(tname1+" "+tname2);
			cpTable = hash(tables, cpTable, relationOperator, on, t1, t2);
			//System.out.println(cpTable);
			System.gc();
			//System.out.println("DOne");
			tableList.add(tname1);
			tableList.add(tname2);
			tname1 = tname2;
		}
		
		where.removeAll(expList);
		joinTable.addAll(cpTable);
		//System.out.println(joinTable);
		cpTable.clear();
		return cpTableInfo;
	}
	
	public LinkedList<LinkedList<String>> hash(HashMap<String, TableInfo> tables,LinkedList<LinkedList<String>> table1,RelationOperator relation, Expression on, int t1, int t2) throws SQLException{
 		LinkedList<LinkedList<String>> table3 = new LinkedList<LinkedList<String>>();
		
 		//System.out.println(t1+"  "+t2);
 		HashMap<String,LinkedList<LinkedList<String>>> hashbuck=new HashMap<String,LinkedList<LinkedList<String>>>();
 		
 		if(on!=null)
 		{
 			for(LinkedList<String> tuple2:table1)
 			{
 				if(!hashbuck.containsKey(tuple2.get(t1)))
 				{
 					LinkedList<LinkedList<String>> list=new LinkedList<LinkedList<String>>();
 					list.add(tuple2);
 					hashbuck.put(tuple2.get(t1), list);
 				}
 				else
 				{
 					LinkedList<LinkedList<String>> list=hashbuck.get(tuple2.get(t1));
 					list.add(tuple2);
 					hashbuck.put(tuple2.get(t1), list);
 				}
 			}
 			
 	 		LinkedList<String> tuple2 = null;

 			while((tuple2=relation.getNext())!=null)
 			{ 			
 				if(hashbuck.containsKey(tuple2.get(t2)))
 				{
 					LinkedList<LinkedList<String>> list=hashbuck.get(tuple2.get(t2));
 					for(LinkedList<String> q:list)
 					{
 						LinkedList<String> temp=new LinkedList<String>();
 		 				
 						temp.addAll(q);
 						temp.addAll(tuple2);
 						if(temp.size()>0)
 							table3.add(temp);
 					}
 				}
 			}
 			//System.out.println(hashbuck);
 		}
 		else
 		{
 			LinkedList<String> tuple2 = null;
 			while((tuple2=relation.getNext())!=null)
 			{ 
 				
	 			for(LinkedList<String> tuple1:table1)
	 			{
					LinkedList<String> temp=new LinkedList<String>();
		 			temp.addAll(tuple1);
					temp.addAll(tuple2);
					table3.add(temp);
	 			}
 			}
 		}
 
 		//System.out.println(table3);
		return table3;
	}
	
	public TableInfo newSchema(TableInfo newTableInfo, TableInfo oldTableInfo){
		//System.err.println(oldTableInfo);
		//System.out.println(oldTableInfo.getTupleSchema());
		String t = oldTableInfo.getTable().getName().toLowerCase();
		if(oldTableInfo.getTable().getAlias() != null)
			t = oldTableInfo.getTable().getAlias();
		int size = newTableInfo.getTupleSchema().size();
		for(Map.Entry<String, Integer> entry : oldTableInfo.getTupleSchema().entrySet()){
			if(!entry.getKey().contains(".")){
				String newColName = t + "." +entry.getKey();
				//Integer newColID = size/2 + entry.getValue();
				Integer newColID = size + entry.getValue();
				newTableInfo.getOldTupleSchema().put(entry.getKey(), newColID);
				newTableInfo.getTupleSchema().put(newColName, newColID);
				//newColName = entry.getKey();
				//newTableInfo.getOldTupleSchema().put(entry.getKey(), newColID);
				//newTableInfo.getTupleSchema().put(newColName, newColID);
			}
		}
		
		size = newTableInfo.getSchema().size();
		for(Map.Entry<String, Datatype> entry : oldTableInfo.getSchema().entrySet()){
			String newColName = t + "." +entry.getKey();
			Datatype newCD = entry.getValue();
			newTableInfo.getOldSchema().put(entry.getKey(), newCD);
			newTableInfo.getSchema().put(newColName, newCD);
			//newColName = entry.getKey();
			//newTableInfo.getOldSchema().put(entry.getKey(), newCD);
			//newTableInfo.getSchema().put(newColName, newCD);
		}
		//System.out.println(newTableInfo.getTupleSchema());
		return newTableInfo;
	}
	
	public LinkedList<String> getNext()throws SQLException{
		if(!(joinTable.empty()))
			return joinTable.pop();
		return null;
	}
	
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		Integer colID = cpTableInfo.getTupleSchema().get(arg0.getWholeColumnName());
		Datatype dataType = this.cpTableInfo.getSchema().get(arg0.getWholeColumnName());
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
	public LinkedList<Expression> getWhere() {
		return where;
	}

	public void setWhere(LinkedList<Expression> where) {
		this.where = where;
	}
	
	public List<Join> getRelationList() {
		return relationList;
	}

	public void setRelationList(List<Join> relationList) {
		this.relationList = relationList;
	}
	 
}
