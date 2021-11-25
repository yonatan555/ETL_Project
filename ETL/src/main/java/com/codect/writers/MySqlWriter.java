package com.codect.writers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MySqlWriter extends Writer {
	Connection con = null;

	@Override
	public void close() throws IOException {
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(List<Map<String, Object>> next) {
		Map<String, Object> exampleRec = next.get(0);
		List<String> columns = new ArrayList<String>(exampleRec.keySet());
		int size = exampleRec.size();
		String query = buildQuery(columns);
		try {
			PreparedStatement pstmt = con.prepareStatement(query);
			for (Map<String, Object> map : next) {
				for (int i = 0; i < size; i++) {
					pstmt.setObject(i + 1, map.get(columns.get(i)));
				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String buildQuery(List<String> columns) {
		String query = "insert into " + (String) target.get("table");
		PreparedStatement prepareStatement;
		String fullColumn = " (";
		String fullValues = " values(";
		for (int i = 0; i < columns.size(); i++) {
			String key= columns.get(i);
			if (i>0) {
				fullValues += ",";
				fullColumn += ",";
			}
			fullColumn += key;
			fullValues += "?";
		}
		fullColumn += ") ";
		fullValues += ");";
		query += fullColumn + fullValues;
		return query;
	}

	@Override
	public void init() {
		String driver = "com.mysql.cj.jdbc.Driver";
		try {
			Class.forName(driver);
			String url = (String) this.target.get("connectionUrl");
			con = DriverManager.getConnection(url, "root", "root");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Connected");
	}

	@Override
	public void prepareTarget(Map<String, Object> map) {
		String tableName = (String) target.get("table");
		Set<String> keySet = map.keySet();
		Collection<Object> values = map.values();
		String createTable = "CREATE TABLE " + " IF NOT EXISTS " + tableName + " (";
		Iterator<String> colIterator = keySet.iterator();
		while (colIterator.hasNext()) {
			String col = colIterator.next();
			Object val = map.get(col);
			String type = getType(val);
			if (colIterator.hasNext()) {
				createTable += col + " " + type + ", ";
			} else
				createTable += col + " " + type + ");";
		}
		try {
			Statement createStatement = con.createStatement();
			createStatement.execute(createTable);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getType(Object val) {
		if (val instanceof String) {
			return "VARCHAR(100)";
		} else if (val instanceof Integer) {
			return "INT(10)";
		} else if (val instanceof Double) {
			return "DOUBLE(10)";
		} else if (val instanceof Date) {
			return "DATE";
		}
		return "VARCHAR(10)";
	}
}
