package main;

import java.util.ArrayList;

import store_data_manager.StoreDataManager;
import urSQL.System.TableAttribute;
import urSQL.System.TableMetadata;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		StoreDataManager sdm = new StoreDataManager();
		
		//sdm.createDatabaseScheme("esquemita");
		
		
		TableAttribute ta1 = new TableAttribute("Nombre", TableAttribute.TYPE_VARCHAR);
		TableAttribute ta2 = new TableAttribute("Apellido", TableAttribute.TYPE_CHAR);
		TableAttribute ta3 = new TableAttribute("Cedula", TableAttribute.TYPE_INT);
		TableAttribute ta4 = new TableAttribute("K/D Ratio", TableAttribute.TYPE_DECIMAL);
		
		ArrayList<TableAttribute> list = new ArrayList<TableAttribute>();
		
		list.add(ta1);
		list.add(ta2);
		list.add(ta3);
		list.add(ta4);
		
		TableMetadata tmd = new TableMetadata("Tablita");
		
		tmd.setPrimaryKey(ta3);
		
		tmd.setTableColumns(list);
		
		//sdm.createTable("esquemita", tmd);
		
		//String[] data = {"Andres", "Brais", "604170973", "5.468"};
		
		//sdm.insertRow("esquemita", tmd, data);
		
		String row = sdm.getRow("604170973", "esquemita", "Tablita");
		
		System.out.println("Fila: " + row);
		
	}

}
