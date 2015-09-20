package store_data_manager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Vector;

import NET.sourceforge.BplusJ.BplusJ.xBplusTreeBytes;
import datamanagement.Pair;

/**
 * Objeto que administra la forma de adminstrar los 
 * archivos de las tablas y las bases de datos
 * 
 * @author Andres Brais 
 *
 */
public class StoreDataManager {
	/*******************STRINGS PARA ARCHIVOS***********************/
	
	/**
	 * Separador de en las direcciones de archivos
	 */
	private static final String FILE_SEPARATOR = File.separator;
	/**
	 * Este es la direcci�n donde se van a almacenar las bases de datos.
	 */
	private static final String DATABASES_PATH = System.getProperty("user.dir") + FILE_SEPARATOR +"DatabasesSchemes";
	/**
	 * Sufijo del nombre de los archivos de arbol
	 */
	private static final String TREE_SUFIX = "_TREE";
	/**
	 * Sufijo del nombre de los archivos de bloques de informacion
	 */
	private static final String BLOCKS_SUFFIX = "_BLOCKS";
	/**
	 * Llave que se usa siempre para la cantidad de columnas en 
	 * una tabla
	 */
	
	/***************************STRINGS PARA TIPOS*****************************/
	
	/**
	 * Tipo que representa el entero
	 */
	public static final String TYPE_INTEGER = "INTEGER";
	/**
	 * Tipo que representa el double 
	 */
	public static final String TYPE_DECIMAL = "DECIMAL";
	/**
	 * Tipo que representa el char
	 */
	public static final String TYPE_CHAR = "CHAR";
	/**
	 * tipo que representa el varchar
	 */
	public static final String TYPE_VARCHAR = "VARCHAR";
	/**
	 * Representa el tipo nulo 
	 */
	public static final String TYPE_NULL = "NULL";
	
	/**************************TIPOS EN BYTES******************************/
	
	private static final byte BY_TYPE_NULL = (byte)0x00;
	
	private static final byte BY_TYPE_INTEGER = (byte)0x01;
	
	private static final byte BY_TYPE_CHAR = (byte)0x02;
	
	private static final byte BY_TYPE_VARCHAR = (byte)0x03;
	
	private static final byte BY_TYPE_DECIMAL = (byte)0x04;
	
	/**************************KEYS DE CONTROL****************************/
	
	/**
	 * Ubica la posicion de la cantidad de columnas 
	 */
	private static final String COLUMN_QUANTITY_KEY = " COLQ";
	/**
	 * Ubica el indice de la llave primaria
	 */
	private static final String PK_INDEX = " PK"; 
	
	/**
	 * Crea la carpeta de las bases de datos si estas no existen.
	 */
	public StoreDataManager(){
		//Archivo de donde esta la base
		File databases_dir = new File(StoreDataManager.DATABASES_PATH);
		
		//si el arhivo no existe lo crea 
		if(!databases_dir.exists()){
			boolean result = false;
			result = databases_dir.mkdirs();
			//si no se pudo crear env�a un mensaje de error
			if(!result){
				System.err.format("Hubo un problema al tratar de crear los siguientes directorios %s\n", 
						StoreDataManager.DATABASES_PATH);
			}
			
		}
	}
	
	/**
	 * Crea la nueva base de datos 
	 * 
	 * @param database_name nombre de nueva base de 
	 * datos 
	 */
	public void createDatabaseScheme(String database_name){
		//se crea el archivo de donde iria el archivo
		File database = new File(StoreDataManager.DATABASES_PATH + "/" + database_name);
		//si la base de datos ya esta creada
		if(database.exists()){
			System.err.format("La base de datos con el nombre %s ya ha sido creada\n", database_name);
		}
		//si no crea el archivo 
		else{
			boolean result = database.mkdirs();
			if(result){
				System.out.format("La base de datos %s fue creada correctamente\n", database_name);
			}else{
				System.err.format("Hubo un error al crear la base de datos %s\n", database_name);
			}
		}
	}
	
	/**
	 * Elimina una base de datos
	 * 
	 * @param database_name Nombre de la base a eliminar
	 */
	public void deleteDatabaseScheme(String database_name){
		//se crea el archivo de donde iria el archivo
		File database = new File(StoreDataManager.DATABASES_PATH + "/" + database_name);
		//se verifica is existe
		if(!database.exists()){
			System.err.format("La base de datos %s no existe en las bases de datos almacenadas\n", database_name);
		}
		//si si existe se procede a elimnar
		else{
			//si lo que se va a borrar no es un directorio
			if(!database.isDirectory()){
				System.err.format("La direccion especificada %s, no corresponde a un directorio\n" , database_name);
			}
			//si cumple con ser un directorio
			else{
				recursiveFileDelete(database);
			}
		}
	}
	
	/**
	 * Elimina la carpeta, elimimna los archivos dentro
	 * si es necesario o si hay en el directorio.
	 * 
	 * @param database Nombre de la base de datos a 
	 * eliminar.
	 */
	private void recursiveFileDelete(File database){
		//Esta pila es utilizada para eliminar los archivos 
		//dentro de los directorios
		java.util.Stack<File> stack = new java.util.Stack<File>();
		//Se agrega el directorio inicial
		stack.push(database);
		
		while(!stack.isEmpty()){
			File tmp = stack.pop();
			//si es un archivo se borra inmediatamente
			if(tmp.isFile()){
				tmp.delete();
			}
			//si es un directorio
			else if(tmp.isDirectory()){
				File[] tmp_array = tmp.listFiles();
				//si no es directorio o un error de I/O
				if(tmp_array == null){
					System.err.format("El archivo de nombre %s, no es un directorio o hubo un error de I/O"
							+ "\nla direccion abosulta es %s\n", 
							tmp.getName(), tmp.getAbsolutePath());
					break;
				}
				//si el directorio esta vacio se elimina inmediatamente
				else if(tmp_array.length == 0){
					tmp.delete();
				}
				//si el directorio esta lleno
				else if(tmp_array.length > 0){
					stack.push(tmp);
					
					for (int i = 0; i < tmp_array.length; i++) {
						//si es un archivo se elimina directamente
						if(tmp_array[i].isFile()){
							tmp_array[i].delete();
						}
						//si es un directorio se agrega a la pila
						else if(tmp_array[i].isDirectory()){
							stack.push(tmp_array[i]);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Crea una table en una base de datos existente.
	 * 
	 * @param table_name nombre de la tabla a crear.
	 * 
	 * @param col_quant cantidad de columnas de la tabla.
	 * 
	 * @param pk_index posicion del vector donde se 
	 * encuentra la llave primaria.
	 * 
	 * @param dabase_name nombre de la base de datos
	 * en la que se va a crear la tabla.
	 */
	public void createTable(String table_name, int col_quant, int pk_index, String database_name){
		//se crea el directorio a ver si existe
		File database = new File(DATABASES_PATH + FILE_SEPARATOR + database_name);
		
		if(!database.exists()){
			System.err.format("La base de datos con el nombre %s\n"
					+ "no se encuentra o no ha sido creada\n", database_name);
		}
		//Si el archivo existe
		else if(database.exists()){
			File tree_file = new File(database, table_name + TREE_SUFIX); 
			File block_file = new File(database, table_name + BLOCKS_SUFFIX);
			
			try {
				xBplusTreeBytes tree = xBplusTreeBytes.Initialize(new RandomAccessFile(tree_file, "rw"),
						new RandomAccessFile(block_file, "rw"), 10);
				//guarda en el arbol la cantidad de columnas 
				short colq_q_sh = (short) col_quant;
				byte[] colq_quant_by = ByteBuffer.allocate(2).putShort(colq_q_sh).array();
				
				tree.set(COLUMN_QUANTITY_KEY, colq_quant_by);
				
				//agrega el indice de la llave primaria
				short pk_index_sh = (short) pk_index;
				byte[] pk_index_by = ByteBuffer.allocate(2).putShort(pk_index_sh).array();
				
				tree.set(PK_INDEX, pk_index_by);
				
				//se envia todo al arbol
				tree.Commit();
				
				if(tree.ContainsKey(COLUMN_QUANTITY_KEY) && tree.ContainsKey(PK_INDEX)){
					System.out.format("La tabla %s ha sido creada correctamente\n", table_name);
				}else{
					System.err.format("Hubo un problema al crear la tabla %s\n", table_name);
				}
				
				//cierra el arbol
				tree.Shutdown();
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.format("Hubo un error al crear el archivo de la tabla %s\n", table_name);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.format("Hubo un error satanico al crear el arbol de la tabla %s\n", table_name);
			}
		}
	}

	/**
	 * Agrega una fila en una tabla de una base de datos.
	 * 
	 * @param database_name Nombre de la base de datos.
	 *  
	 * @param table_name Nombre de la tabla.
	 * 
	 * @param vec Vector qeu contiene los tipos y datos a 
	 * insertar en la tabla.
	 */
	public void insertRow(String database_name, String table_name, Vector<Pair<String,String>> vec ){
		//se verifica que exista la carpeta de bases de datos
		File file_tree = new File(DATABASES_PATH + FILE_SEPARATOR + database_name);
		if(!file_tree.exists()){
			//la base de datos no existe
			System.err.format("La base de datos con el nombre %s no ha sido creada\n", database_name);
		}
		else{
			//se verifica que existan los archivos de las tablas 
			File file_blocks = new File(file_tree, table_name + BLOCKS_SUFFIX);
			file_tree = new File(file_tree, table_name + TREE_SUFIX);
			if(!file_blocks.exists() || !file_tree.exists()){
				System.err.format("La tabla con el nombre %s no ha sido creada\n"
						+ "o algun archivo a sido corrompido\n", table_name);
			}
			else{
				//si existen se crea el arbol
				try {
					xBplusTreeBytes tree = xBplusTreeBytes.ReOpen(new RandomAccessFile(file_tree, "rw"), 
							new RandomAccessFile(file_blocks, "rw"));
					//se busca la cantidad de columnas
					byte[] b_columns = tree.get(COLUMN_QUANTITY_KEY);
					short columns_q = ByteBuffer.wrap(b_columns).getShort();
					//se obtiene el indice de la llave primaria
					byte[] b_pk_index = tree.get(PK_INDEX);
					short pk_index = ByteBuffer.wrap(b_pk_index).getShort();
					
					if(vec.size() != columns_q){
						System.err.format("La fila debe tener %d columnas \n", (int)columns_q);
					}
					//si cumple con la cantidad de columnas
					else {
						
						insertRowAux(pk_index, tree ,vec);
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Recibe los datos sin ser pasados a bytes y los transforma,
	 * ademas de que encuentra la llave primaria
	 * 
	 * @param pk_index poscion de la llave primaria
	 * 
	 * @param tree arbol donde se va a almacenar los valores de
	 * la fila
	 * 
	 * @param vec Vector con los pares de tipo e informaci�n
	 */
	private void insertRowAux(int pk_index, xBplusTreeBytes tree ,Vector<Pair<String,String>> vec){
		//si la llave primaria es nula
		if(vec.get(pk_index).getFirst().compareTo(TYPE_NULL) == 0){
			System.err.format("La llave primaria es nula\n");
		}
		else{
			//llave de la fila
			String key = vec.get(pk_index).getSecond();
			
			try {
				if(tree.ContainsKey(key)){
					System.err.format("En la tabla que quiere insertar la llave primaria %s ya fue insetada", key);
				}
				else{
					//transforma todo a bytes
					byte[] reg =  toBytes(vec);
					//escribe los datos en el arbol
					try {
						//agrega el registro
						tree.set(key, reg);
						tree.Commit();
						//se comprueba que este en el arbol
						if(!tree.ContainsKey(key)){
							System.err.format("La fila con la llave %s no se inserto correctamente\n", key);
						}
						else{
							System.out.format("La fila con la llave %s fue insertada con exito\n", key);
							System.out.format("Tama�o del registro: %d\n", tree.get(key).length);
						}
						//cierra el arbol
						tree.Shutdown();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Pasa a bytes la informaci�n que se quiere almacenar
	 * en la tabla.
	 * 
	 * @param vec Vector con el pares de string donde 
	 * el primer objeto es el tipo del objeto, y el 
	 * segundo es la informaci�n a almacenar
	 * 
	 * @return un arreglo de bytes con toda la informaci�n
	 */
	private byte[] toBytes(Vector<Pair<String,String>> vec){
		//crea un nuevo vector donde se alamacenan los arreglos
		//de bytes para ser concatenados
		Vector<byte[]> res = new Vector<byte[]>();
		//toma todos los datos y los convierte a  bytes
		for (int i = 0; i < vec.size(); i++) {
			String type = vec.get(i).getFirst();
			
			String data = vec.get(i).getSecond();
			
			byte[] tmp_r = toBytesAux(type,data);
					
			res.add(tmp_r);
		}
		//concatena todos los arreglos
		return concatenateByteArray(res);
	}
	
	/**
	 * Retorna un arreglo de bytes donde se concatenaron todos
	 * los subregistros de informaci�n con su debido enccabezado
	 * 
	 * @param bytes vector con todos los arreglos de bytes que 
	 * van a ser concatenados.
	 * 
	 * @return un arreglo de bytes cono resultado de la concatenaci�n
	 * de todos los arreglos de bytes
	 */
	private byte[] concatenateByteArray(Vector<byte[]> bytes){
		//tama�o total del arreglo de bytes
		int total_size = 0;

		for (int i = 0; i < bytes.size(); i++) {
			total_size += bytes.get(i).length;
		}
		//se crea el arreglo con el tama�o total
		byte[] register = new byte[total_size];
		//indice que se lleva sobre el arreglo final
		int global_index = 0;
		
		for (int i = 0; i < bytes.size(); i++) {
			byte[] sub_register = bytes.get(i);
			
			for (int j = 0; j < sub_register.length; j++) {
				register[global_index+j] = sub_register[j];
			}
			global_index += sub_register.length;
		}
		
		return register;
		
	}
	
	
	/**
	 * Se pasan todos los tipos con su respectivo encabezado
	 * para ser leidos cuando son extra�dos
	 * 
	 * @param type Tipo de informaci�n que se almacena dentro
	 * en el registro
	 * 
	 * @param data informaci�n que se almacena en el registro
	 * 
	 * @return un arreglo de bytes que representa al registro 
	 * completo
	 */
	private byte[] toBytesAux(String type, String data){
		switch(type){
			case TYPE_INTEGER:
				int integer = Integer.parseInt(data);
				byte[] integer_array = int2bytes(integer);
				return makeRegister(BY_TYPE_INTEGER, integer_array.length, integer_array);
				
			case TYPE_NULL:
				byte[] null_array = {(byte)0x00};
				return makeRegister(BY_TYPE_NULL, null_array.length, null_array);
				
			case TYPE_DECIMAL:
				float float_value = Float.parseFloat(data);
				byte[] float_array = float2bytes(float_value);
				return makeRegister(BY_TYPE_DECIMAL, float_array.length, float_array);
				
			case TYPE_CHAR:
				byte[] char_array = data.getBytes();
				return makeRegister(BY_TYPE_CHAR, char_array.length, char_array);
				
			default:
				byte[] string_array = data.getBytes();
				return makeRegister(BY_TYPE_VARCHAR, string_array.length, string_array);
		}
	}
	
	/**
	 * Crea el registro que se de la fila
	 * 
	 * @param type tipo de la informaci�n que se va a guardar
	 * en el registro
	 * 
	 * @param size cantidad de bytes que va a ocupar el registro
	 * 
	 * @param data informaci�n del registro 
	 * 
	 * @return arreglo de bytes donde se contiene la informacion,
	 * el tama�o y el tipo de informacion guardada
	 */
	private byte[] makeRegister(byte type, int size, byte[] data){
		//se crea el registro donde se almacena  
		byte[] reg = new byte[3+data.length];
		//se agrega primero el tipo
		reg[0] = type;
		//se agrega el tama�o
		byte[] by_size = short2bytes((short)size);
		reg[1] = by_size[0];
		reg[2] = by_size[1];
		//se agrega la informaci�n
		for (int i = 0; i < data.length; i++) {
			reg[i+3] = data[i];
		}
		
		return reg;
	}
	
	/**
	 * Busca una fila en el arbol por la llave primaria 
	 * y la devuelve en String
	 * 
	 * @param pk Llave primaria
	 * 
	 * @param database_name nombre de la base de datos 
	 * en la que se encuentra la tabla
	 * 
	 * @param table_name tabla a la que se le va a 
	 * leer una fila
	 * 
	 * @return String con la fila.
	 */
	public String getRow(String pk, String database_name, String table_name){
		String result = "";
		//se verifica que exista la carpeta de bases de datos
		File file_tree = new File(DATABASES_PATH + FILE_SEPARATOR + database_name);
		if(!file_tree.exists()){
		//la base de datos no existe
			System.err.format("La base de datos con el nombre %s no ha sido creada\n", database_name);
		}
		else{
			//se verifica que existan los archivos de las tablas 
			File file_blocks = new File(file_tree, table_name + BLOCKS_SUFFIX);
			file_tree = new File(file_tree, table_name + TREE_SUFIX);

			if(!file_blocks.exists() || !file_tree.exists()){
				System.err.format("La tabla con el nombre %s no ha sido creada\n"
						+ "o algun archivo a sido corrompido\n", table_name);
			}
			else{
				//si existen los archivos se crea el arbol
				try {
					xBplusTreeBytes tree = xBplusTreeBytes.ReOpen(new RandomAccessFile(file_tree, "rw"), 
							new RandomAccessFile(file_blocks, "rw"));
					//comprueba que la llave se encuentre en el arbol
					if(!tree.ContainsKey(pk)){
						System.err.format("En la tabla %s de la base de datos %s no se\n"
								+ "encuentra la llave primaria %s\n", table_name, database_name, pk);
					}
					//si se encuentra toma el registro
					else{
						byte[] register = tree.get(pk);
						//apaga el arbol
						tree.Shutdown();
						//se convierte a string
						result = parseByteArray2String(register);
					}
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		return result;
	}
	
	/**
	 * Convierte el registro de byte en un string
	 * contiendo los valores de todos las columnas
	 * 
	 * @param bytes registro que se va a parsear
	 * 
	 * @return String con la forma de la fila
	 */
	private String parseByteArray2String(byte[] bytes){
		String result = "";
		//indice por el cual va 
		int current_index = 0;
		//offset de por donde se va a leer
		int offset = 3;
		//cantidad de bytes que se van a leer
		int length = 0;
		//tipo de valor del que se va a leer
		byte type = (byte)0x00;
		
		while(current_index < bytes.length){
			//se toma el tipo
			type = bytes[current_index];
			//se toma cuantos bytes ocupa
			length = (int)ByteBuffer.wrap(bytes, current_index+1, 2).getShort();
			//se obtiene el valor en string
			String tmp = byteSwitch(bytes, offset, length, type);
			//se hace apend de lo que se lee
			result = result.concat("|"+ tmp +"|");
			//se establecen los valores para el proximo subregistro
			current_index = current_index + length + 3;
			offset = offset + length + 3;
		}
		
		return result;
	}
	
	/**
	 * Lee los bytes dados con los limistes dados para crear
	 * un tipo y transformarlo a {@link String} 
	 * 
	 * @param bytes arrglo de bytes que se van a 
	 * transforman a {@link String}
	 * 
	 * @param offset corrimiento en el arreglo de bytes
	 * 
	 * @param length cantidad de bytes que van a ser leidos
	 * 
	 * @param type tipo de la informacion almacenada
	 * 
	 * @return String con lo que se leyo en el subregistro
	 */
	private String byteSwitch(byte[] bytes, int offset, int length, byte type){
		switch(type){
			case(BY_TYPE_NULL):
				return "NULL";
			case(BY_TYPE_INTEGER):
				int num_int = ByteBuffer.wrap(bytes).getInt(offset);
				return String.valueOf(num_int);
			case(BY_TYPE_DECIMAL):
				float num_fl = ByteBuffer.wrap(bytes).getFloat(offset);
				return String.valueOf(num_fl);
			default:
				return new String(bytes, offset, length);
		}
	}
	
	
	/**
	 * Convierte un entero a arreglo de bytes
	 * 
	 * @param integer Entero a convertir
	 * 
	 * @return arreglo de bytes que representa al entero
	 */
	private byte[] int2bytes(int num){
		return ByteBuffer.allocate(4).putInt(num).array();
	}
	
	/**
	 * Convierte un short a un arreglo de bytes
	 * 
	 * @param num short que se va a convertir
	 * 
	 * @return arreglo de bytes que representa el short 
	 */
	private byte[] short2bytes(short num){
		return ByteBuffer.allocate(2).putShort(num).array();
	}
	
	/**
	 * Convierte un float a un arreglo de bytes
	 * 
	 * @param num float que se va a convertir
	 * 
	 * @return arreglo de bytes que representa el float
	 */
	private byte[] float2bytes(float num){
		return ByteBuffer.allocate(4).putFloat(num).array();
	}
	
}
