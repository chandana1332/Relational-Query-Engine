package edu.buffalo.cse562;

import java.util.HashMap;

import net.sf.jsqlparser.schema.Table;

public class TableInfo {
	private Table table;
	private HashMap<String, Integer> tupleSchema;
	private HashMap<String, Integer> oldTupleSchema;
	private HashMap<String, Datatype> schema;
	private HashMap<String, Datatype> oldSchema;
	private boolean first;
	
	public TableInfo(){
		this.table = new Table();
		this.tupleSchema = new HashMap<String, Integer>();
		this.oldTupleSchema = new HashMap<String, Integer>();
		this.schema = new HashMap<String, Datatype>();
		this.oldSchema = new HashMap<String, Datatype>();
		this.first = true;
	}
	public HashMap<String, Integer> getTupleSchema() {
		return tupleSchema;
	}
	public void setTupleSchema(HashMap<String, Integer> tupleSchema) {
		this.tupleSchema = tupleSchema;
	}
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public HashMap<String, Datatype> getSchema() {
		return schema;
	}
	public void setSchema(HashMap<String, Datatype> schema) {
		this.schema = schema;
	}
	public HashMap<String, Integer> getOldTupleSchema() {
		return oldTupleSchema;
	}
	public void setOldTupleSchema(HashMap<String, Integer> oldTupleSchema) {
		this.oldTupleSchema = oldTupleSchema;
	}
	public HashMap<String, Datatype> getOldSchema() {
		return oldSchema;
	}
	public void setOldSchema(HashMap<String, Datatype> oldSchema) {
		this.oldSchema = oldSchema;
	}
	public boolean isFirst() {
		return first;
	}
	public void setFirst(boolean first) {
		this.first = first;
	}
	
}
