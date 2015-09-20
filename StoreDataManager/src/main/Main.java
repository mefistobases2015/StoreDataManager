package main;


import java.util.Vector;

import datamanagement.Pair;
import store_data_manager.StoreDataManager;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		StoreDataManager sdm = new StoreDataManager();
		
		//sdm.createDatabaseScheme("esquemita2");
		
		//sdm.createTable("tablita", 5, 0, "esquemita2");
		
		Pair<String,String> pair0 = new Pair<String,String>(StoreDataManager.TYPE_VARCHAR, "607890123");
		
		Pair<String,String> pair1 = new Pair<String,String>(StoreDataManager.TYPE_VARCHAR, "Keylor");
		
		Pair<String,String> pair2 = new Pair<String,String>(StoreDataManager.TYPE_VARCHAR, "Pagano");
		
		Pair<String,String> pair3 = new Pair<String,String>(StoreDataManager.TYPE_NULL, "");
		
		Pair<String,String> pair4 = new Pair<String,String>(StoreDataManager.TYPE_DECIMAL, "40.8945");
		
		Vector<Pair<String,String>> vec = new Vector<Pair<String, String>>();
		
		vec.add(pair0);
		vec.add(pair1);
		vec.add(pair2);
		vec.add(pair3);
		vec.add(pair4);
		
		sdm.insertRow("esquemita2", "tablita", vec);
		
		String row = sdm.getRow("607890123", "esquemita2", "tablita");
		
		System.out.println("Row: " + row);
	}

}
