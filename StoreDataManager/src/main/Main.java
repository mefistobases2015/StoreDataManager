package main;

import java.util.ArrayList;
import java.util.Iterator;

import store_data_manager.StoreDataManager;
import urSQL.System.TableAttribute;
import urSQL.System.TableMetadata;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		StoreDataManager sdm = new StoreDataManager();
		
		//sdm.createDatabase("esquemita");
		
		
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
		
		/*String[] data = {"Arturo", "Mora", "103650471", "2.156"};
		
		sdm.insertRow("esquemita", tmd, data);
		
		String[] data2 = {"Manuel", "Mora", "104370624", "1.2365"};
		
		sdm.insertRow("esquemita", tmd, data2);
		
		String[] data3 = {"Hector", "Porras", "105630624", "2.314"};
		
		sdm.insertRow("esquemita", tmd, data3);*/
		
		printTable(sdm.getTable("esquemita", "Tablita"));
		
		//String[] n_data = {"Andres", "Brais", "604170973", "8.5648"};
		
		//sdm.updateRegister("esquemita", "Tablita", "604170973", n_data);
		
		sdm.deleteRow("esquemita", "Tablita", "104370624");
		
		printTable(sdm.getTable("esquemita", "Tablita"));
		
	}
	
	public static void printList(ArrayList<String> row){
		Iterator<String> iterator = row.iterator();
		
		while(iterator.hasNext()){
			System.out.print("|" + iterator.next() +"|");
		}
		
	}
	
	public static void printTable(ArrayList<ArrayList<String>> table){
		Iterator<ArrayList<String>> iterator = table.iterator();
		
		while(iterator.hasNext()){
			printList(iterator.next());
			System.out.println();
		}
	}

}
