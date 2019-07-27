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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
	@CrossOrigin(origins ="*")
	String getAccount() {
		String result =null;
		String sqlStmt = "Select * From Accounts";
        System.out.print(new Date().getTime());
		System.out.println("getAllAccount");
		try {
			initDatabase();

			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;
			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result;

	}

	@RequestMapping("/getEvent")
	@CrossOrigin(origins ="*")
	String getEvent() {
		String sqlStmt = "Select * from Events";
		String result = null;
        System.out.print(new Date().getTime());
		System.out.println("GetEvent");
		try {
			initDatabase();

			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}
	
	@RequestMapping("/getUserEvents")
	@CrossOrigin(origins ="*")
	String getEventUser(@RequestParam("userId") String userId) {
		String sqlStmt = "	SELECT t1.eventId,organiserId,TIMESTAMPDIFF(hour,t1.startTime,t1.endTime) AS 'Duration'\n" + 
				"	FROM report.Events t1\n" + 
				"	JOIN report.EventRegistrations t2\n" + 
				"	ON t1.eventId = t2.eventId\n" + 
				"	WHERE t2.userId="+userId;
		String result = null;
        System.out.print(new Date().getTime());
		System.out.println("getUserEvents/"+userId);
		try {
			initDatabase();


			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}
	

	@RequestMapping("/getEventUsers")
	@CrossOrigin(origins ="*")
	String getEventUsers(@RequestParam("eventId") String eventId) {
		String sqlStmt = "SELECT t3.nationality, t3.gender, t3.birthDate, t3.region\n" + 
				"	FROM report.Events t1\n" + 
				"	JOIN report.EventRegistrations t2 ON t1.eventId=t2.eventId\n" + 
				"	JOIN report.Accounts t3 ON t2.userId=t3.userId\n" + 
				"	WHERE t1.eventId="+eventId;
		String result = null;
        System.out.print(new Date().getTime());
		System.out.println("getEventUsers/"+eventId);
		try {
			initDatabase();


			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}
	
	
	@RequestMapping("/getEventFeedback")
	@CrossOrigin(origins ="*")
	String getEventFeedback(@RequestParam("eventId") String eventId) {
		String sqlStmt = "SELECT userId, status, feedback\n" + 
				"FROM report.EventRegistrations t1\n" + 
				"WHERE t1.eventId="+eventId;
		String result = null;
		System.out.print(new Date().getTime());
		System.out.println("getEventFeedback/"+eventId);
		try {
			initDatabase();
			
			
			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}

	
	@RequestMapping("/getOrgEvents")
	@CrossOrigin(origins ="*")
	String getOrganisationEvents(@RequestParam("organisationId") String organisationId) {
		String sqlStmt = "SELECT t2.orgName, t2.orgId, t1.eventName, \n" + 
				"t1.eventDesc, t1.signupCount, t1.status, t1.startTime, t1.endTime\n" + 
				"FROM report.Events t1\n" + 
				"JOIN report.Organisations t2\n" + 
				"ON t1.organiserId=t2.orgId\n" + 
				"WHERE t1.organiserId="+organisationId;
		String result = null;
        System.out.print(new Date().getTime());
		System.out.println("getOrgEvents/"+organisationId);
		try {
			initDatabase();


			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
			if (result != null)
				return result;

			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result; 
	}
	
	
	@RequestMapping("/getEventsForPeriod")
	@CrossOrigin(origins ="*")
	String getEventsForPeriod(@RequestParam("startDate") String startDate,@RequestParam("endDate") String endDate) {
		String sqlStmt = "SELECT \n" + 
				"t1.eventId, t1.eventName, t1.startTime, t1.endTime, count(t2.userId) AS numPax\n" + 
				"FROM report.Events t1\n" + 
				"JOIN report.EventRegistrations t2\n" + 
				"WHERE t1.eventId=t2.eventId\n" + 
				"AND t1.startTime between \""+ startDate + "\" AND \""+ endDate +"\"\n" + 
				"AND t1.endTime between \""+ startDate +"\" AND \""+ endDate +"\"\n" +
				"GROUP BY t1.eventId;";
		String result = null;
        System.out.print(new Date().getTime());
		System.out.println("getEventsForPeriod?start="+startDate+"&end="+endDate);
		try {
			initDatabase();


			result = resultSetToJson(con, sqlStmt);
            System.out.println("Pulled data:\n "+result);
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
