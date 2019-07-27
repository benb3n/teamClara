package com.example.demo;

import com.google.gson.Gson;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {

	Connection con;

	void initDatabase() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String username = "CitiAdmin";
			String password = "citihack2019";
			con = DriverManager.getConnection(
					"jdbc:mysql://citihack2019.cwop36kfff9j.ap-southeast-1.rds.amazonaws.com:3306/report", username,
					password);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@RequestMapping("/getAllAccount")
	String getAccount() {
		String result =null;
		String sqlStmt = "Select * From Accounts";
		System.out.println("starting");
		try {
			initDatabase();
			System.out.println("Pulled data");

			result = resultSetToJson(con, sqlStmt);

			if (result != null)
				return result;
			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result;

	}

	@RequestMapping("/getEvent")
	String getEvent() {
		String sqlStmt = "Select * from Events";
		String result = null;
		System.out.println("starting");
		try {
			initDatabase();
			System.out.println("Pulled data");

			result = resultSetToJson(con, sqlStmt);

			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}
	
	
	
	
	public static String resultSetToJson(Connection connection, String query) {
		List<Map<String, Object>> listOfMaps = null;
		try {
			QueryRunner queryRunner = new QueryRunner();
			listOfMaps = queryRunner.query(connection, query, new MapListHandler());
		} catch (SQLException se) {
			throw new RuntimeException("Couldn't query the database.", se);
		} finally {
			DbUtils.closeQuietly(connection);
		}
		return new Gson().toJson(listOfMaps);
	}

}
