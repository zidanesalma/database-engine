package engine;

/** * @author Wael Abouelsaadat */

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings({ "unchecked", "removal", "rawtypes" })
public class DBApp {

	public DBApp() {

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("resources\\DBApp.config"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Page.maxRowCount = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		Collection<String> values = htblColNameType.values();
		for (String value : values) {
			if (!value.equalsIgnoreCase("java.lang.double") &&
					!value.equalsIgnoreCase("java.lang.integer") &&
					!value.equalsIgnoreCase("java.lang.string")) {
				throw new DBAppException("Unsupported Column Type");
			}
		}

		// Create directory where we store tables
		File tableFolder = new File("Tables\\" + strTableName);
		if (!tableFolder.isDirectory() && !tableFolder.mkdirs()) {
			throw new DBAppException("Could not create folder");
		}

		try {

			Path csvPath = Paths.get("Tables\\metadata.csv");
			BufferedWriter csv = new BufferedWriter(new FileWriter(csvPath.toString()));

			// Check if table already exists in metadata
			if (Files.readAllLines(csvPath).stream().skip(0)
					.anyMatch(data -> data.split(",")[0].equals(strTableName))) {
				csv.close();
				throw new DBAppException("Table already exists");
			}

			// If metadata.csv doesn't exist, write the header
			if (Files.size(csvPath) == 0)
				csv.write("Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType\n");

			for (Map.Entry<String, String> column : htblColNameType.entrySet()) {
				String info = strTableName + "," + column.getKey() + "," + column.getValue() + ","
						+ strClusteringKeyColumn.equals(column.getKey()) + ",null,null\n";
				csv.write(info);
			}

			csv.close();

		} catch (IOException e) {
			throw new DBAppException("An error occurred.");
		}

	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException {

		String filePath = "Tables\\metadata.csv";
		String lineToModify = strTableName + "," + strColName;

		try {
			// Read the file into memory and make modifications
			File inputFile = new File(filePath);
			File tempFile = new File("Tables\\tempFile.txt");

			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

			String currentLine;

			while ((currentLine = reader.readLine()) != null) {
				// Replace the line if it matches the line to modify
				if (currentLine.contains(lineToModify)) {
					String[] data = currentLine.split(",");
					data[4] = strIndexName;
					data[5] = "B+tree";

					currentLine = String.join(",", data);

				}
				writer.write(currentLine + System.getProperty("line.separator"));
			}

			// Close resources
			reader.close();
			writer.close();

			// Replace the original file with the modified file
			if (!inputFile.delete()) {
				throw new DBAppException("Could not delete the original file.");
			}
			if (!tempFile.renameTo(inputFile)) {
				throw new DBAppException("Could not rename the temporary file.");
			}
		} catch (IOException e) {
			throw new DBAppException("Error modifying metadata file");
		}
		Table t = new Table(strTableName);
		t.createIndex(strColName, strIndexName);
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table t = new Table(strTableName);

		try {
			t.insert(htblColNameValue);
		} catch (Exception ex) {
			throw new DBAppException(ex.getMessage());
		}

	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Table table = new Table(strTableName);
		table.update(strClusteringKeyValue, htblColNameValue);
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Table t = new Table(strTableName);
		try {
			t.deleteTuple(htblColNameValue);
		} catch (Exception ex) {
			throw new DBAppException(ex.getMessage());
		}
	}

	public boolean validateInputTypes(Object object, String expectedType) {
		return (object.getClass().getName()).equalsIgnoreCase(expectedType);
	}

	public Vector<Tuple> evaluateSqlTerm(Table table, SQLTerm sqlTerm) throws DBAppException {
		int columnIndex = table.getIndexOfColumn(sqlTerm._strColumnName);

		if (columnIndex < 0)
			throw new DBAppException("Column does not exist");

		String columnName = sqlTerm._strColumnName; // column name in condition
		String operator = sqlTerm._strOperator; // operator in sqlTerm

		if (!(operator.equals(">") || operator.equals(">=") ||
				operator.equals("<") || operator.equals("<=") ||
				operator.equals("!=") || operator.equals("=")))
			throw new DBAppException("Unsupported Operator!");

		Comparable<Object> value = (Comparable<Object>) sqlTerm._objValue; // value of column we're searching with
		if (!validateInputTypes(value, table.getColumnDataType(columnName)))
			throw new DBAppException("Column type mismatch");

		return table.selectFromTable(columnName, operator, value);
	}

	public SQLTerm doOperation(Table table, SQLTerm term1, SQLTerm term2, String operator) throws DBAppException {
		Vector<Tuple> results = new Vector<>();

		if (operator.equalsIgnoreCase("or")) {
			results.addAll(term1.result);
			results.addAll(term2.result);
		} else if (operator.equalsIgnoreCase("and")) {
			Vector<Tuple> result1 = term1.result;
			Vector<Tuple> result2 = term2.result;
			results.addAll(result1);
			results.retainAll(result2);
		} else {
			// (x-y) or (y-x)
			Vector<Tuple> x = term1.result;
			Vector<Tuple> y = term2.result;

			Vector<Tuple> xy = (Vector<Tuple>) x.clone();
			Vector<Tuple> yx = (Vector<Tuple>) y.clone();

			xy.removeAll(y);
			yx.removeAll(x);

			results.addAll(xy);
			results.addAll(yx);
		}

		Set<Tuple> noDups = new HashSet<>(results);
		return new SQLTerm(new Vector<>(noDups), term1, term2, operator);
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException {

		for (String op : strarrOperators) {
			if (!op.equalsIgnoreCase("and") && !op.equalsIgnoreCase("or") && !op.equalsIgnoreCase("xor"))
				throw new DBAppException("Unsupported operator between sqlterms");
		}

		if (arrSQLTerms.length == 0)
			throw new DBAppException("array of sql terms is empty");

		if (arrSQLTerms.length != strarrOperators.length + 1)
			throw new DBAppException("Invalid query");

		Table table = new Table(arrSQLTerms[0]._strTableName);

		Vector<Object> infix = new Vector<>();
		infix.add(arrSQLTerms[0]);
		for (int i = 0; i < strarrOperators.length; i++) {
			infix.add(strarrOperators[i]);
			infix.add(arrSQLTerms[i + 1]);
		}

		Vector<Object> postfix = PostfixConverter.infixToPostfix(infix);

		Stack<SQLTerm> resultSets = new Stack<>(); // results for sql term

		for (Object object : postfix) {
			if (object instanceof SQLTerm) {
				SQLTerm term = (SQLTerm) object;
				term.result = evaluateSqlTerm(table, term);
				resultSets.push(term);
			} else {
				String operator = (String) object;

				SQLTerm term1 = resultSets.pop();
				SQLTerm term2 = resultSets.pop();

				resultSets.push(doOperation(table, term1, term2, operator));

			}
		}

		Vector<Tuple> result = resultSets.pop().result;
		Collections.sort(result);
		return result.iterator();
	}

	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();
			dbApp.init();

			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");

			dbApp.createTable(strTableName, "id", htblColNameType);
			dbApp.createIndex(strTableName, "gpa", "gpaIndex");

			Hashtable htblColNameValue = new Hashtable();
			
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();

			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();

			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();

			htblColNameValue.put("id", new Integer(23498));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();

			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			SQLTerm[] arrSQLTerms = new SQLTerm[2];
			
			arrSQLTerms[0]=new SQLTerm();  
			arrSQLTerms[1]=new SQLTerm();
			
			arrSQLTerms[0]._strTableName =  "Student"; 
			arrSQLTerms[0]._strColumnName=  "name"; 
			arrSQLTerms[0]._strOperator  =  "="; 
			arrSQLTerms[0]._objValue     =  "John Noor"; 

			arrSQLTerms[1]._strTableName =  "Student"; 
			arrSQLTerms[1]._strColumnName=  "gpa"; 
			arrSQLTerms[1]._strOperator  =  "="; 
			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
			String[] strarrOperators = new String[1];
			strarrOperators[0] = "OR"; 
			// select * from Student where name = “John Noor” or gpa = 1.5; 
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators); 

			System.out.println("Select results:");
			while (resultSet.hasNext())
				System.out.println(resultSet.next());

		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

}