package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.LeafValue;

public interface Operator {
	public LinkedList<String> getNext()throws SQLException;
	public Operator getParent();	
	public void setParent(Operator parent);	
	public Operator getChild();
	public void setChild(Operator child);
}
