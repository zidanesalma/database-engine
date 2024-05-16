package engine;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import BTree.BTreePlus;
import BTree.Pair;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class Table {

	private final String tableName;
	private final Hashtable<String, String> columns;
	private final Hashtable<String, String> indices;

	private String clusteringColumn;
	private int clusteringIndex;

	private Vector<Range> ranges;
	private Vector<String> pagesPath;

	public Table(String name) throws DBAppException {
		pagesPath = new Vector<>();
		ranges = new Vector<>();
		columns = new Hashtable<>();
		tableName = name;
		indices = new Hashtable<>();

		try {

			Files.readAllLines(Paths.get("Tables\\metadata.csv")).stream().skip(1).forEach((line) -> {
				String[] data = line.split(",");

				if (!data[0].equals(tableName))
					return;

				if (Boolean.parseBoolean(data[3])) // check if key is the clustering key
				{
					clusteringColumn = data[1];
					clusteringIndex = columns.size();
				}

				columns.put(data[1], data[2]);

				if (!data[4].equals("null"))
					indices.put(data[1], "Tables\\" + tableName + "\\" + data[4] + ".ser");
			});
		} catch (IOException e) {
			throw new DBAppException("Could not read metadata file");
		}

		if (columns.isEmpty())
			throw new DBAppException("Table does not exist.");

		if (clusteringColumn == null)
			throw new DBAppException("No clustering column??");

		File[] files = new File("Tables\\" + tableName).listFiles();
		if (files == null)
			return;

		for (File file : files)
			if (file.getName().equals("ranges.class"))
				ranges = Serializer.deserializeFrom("Tables\\" + tableName + "\\ranges.class");

		// Filter files that have names such as StudentXX.class (student = table name)
		pagesPath.addAll(Arrays.stream(files)
				.filter(file -> file.getName().contains(".class") && file.getName().contains(tableName))
				.map(File::getPath).toList());
	}

	public String getClusteringColumn() {
		return clusteringColumn;
	}

	public Vector<String> getPagesPath() {
		return pagesPath;
	}

	public Vector<Range> getRanges() {
		return ranges;
	}

	public String getColumnDataType(String colName) {
		return columns.get(colName);
	}

	public Hashtable<String, String> getColumns() {
		return columns;
	}

	public int findPageMightContainTuple(Object pk) {
		int index = Collections.binarySearch(ranges, new Range(pk, pk, false));
		if (index >= 0)
			return index;

		index = -index - 1;

		if (index == ranges.size())
			return -1;

		Range range = ranges.get(index);
		if (range.inRange(pk))
			return index;

		return -1;
	}

	private void insertHelper(Page page, Tuple tuple, int i) throws DBAppException, Exception {

		while (true) {

			Range oldRange = ranges.get(i);
			page.insert(tuple);

			Hashtable<String, BTreePlus> trees = new Hashtable<>();

			for (Map.Entry<String, String> column : indices.entrySet())
				trees.put(column.getKey(), Serializer.deserializeFrom(column.getValue()));

			for (Map.Entry<String, BTreePlus> btree : trees.entrySet()) {
				Object key = tuple.getRecord(getIndexOfColumn(btree.getKey()));
				BTreePlus tree = btree.getValue();

				tree.insert(key, page.getPageIndex());
			}

			if (!page.maximumExceeded()) { // maximum not exceeded
				Serializer.serializeTo(page, pagesPath.get(i));

				Range newRange = page.getRange();
				if (!oldRange.equals(newRange)) {
					ranges.setElementAt(newRange, i);
					Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
				}

				for (String column : indices.keySet()) {
					BTreePlus tree = trees.get(column);
					String path = indices.get(column);

					Serializer.serializeTo(tree, path);
				}

				return;
			}

			// code continues here lw maximum number of tuples is exceeded
			tuple = page.deleteAndGetLastTuple();
			for (Map.Entry<String, BTreePlus> btree : trees.entrySet()) {
				Object key = tuple.getRecord(getIndexOfColumn(btree.getKey()));
				BTreePlus tree = btree.getValue();

				tree.delete(key, page.getPageIndex());
			}

			for (String column : indices.keySet()) {
				BTreePlus tree = trees.get(column);
				String path = indices.get(column);

				Serializer.serializeTo(tree, path);
			}

			Range newRange = page.getRange();
			if (!oldRange.equals(newRange)) {
				ranges.setElementAt(newRange, i);
				Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
			}

			Serializer.serializeTo(page, pagesPath.get(i));
			i++;

			if (i < pagesPath.size()) { // lw fe page ba3d de
				page = Serializer.deserializeFrom(pagesPath.get(i));
				continue;
			}

			// maximum is exceeded and no more pages
			String tablePath = "Tables\\" + tableName + "\\" + tableName;
			int lastPath = Integer.parseInt(pagesPath.getLast().replace(tablePath, "").replace(".class", ""));

			page = new Page(clusteringIndex, lastPath + 1);
			String pagePath = page.getPagePath(this.tableName);
			pagesPath.add(pagePath);

			page.insert(tuple);
			for (Map.Entry<String, BTreePlus> btree : trees.entrySet()) {
				Object key = tuple.getRecord(getIndexOfColumn(btree.getKey()));
				BTreePlus tree = btree.getValue();

				tree.insert(key, page.getPageIndex());
			}

			for (String column : indices.keySet()) {
				BTreePlus tree = trees.get(column);
				String path = indices.get(column);

				Serializer.serializeTo(tree, path);
			}

			ranges.add(page.getRange());

			Serializer.serializeTo(page, pagePath);
			Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
			break;
		}
	}

	public void insert(Hashtable<String, Object> records) throws DBAppException, Exception {

		for (Map.Entry<String, String> column : columns.entrySet()) {
			Object record = records.get(column.getKey());
			if (record == null)
				throw new DBAppException("Missing column(s) in record");

			String type = record.getClass().getName();

			if (!type.equalsIgnoreCase(column.getValue()))
				throw new DBAppException("Column type mismatch??");
		}

		Tuple tuple = new Tuple(records, clusteringIndex);
		Page page;
		if (pagesPath.size() == 0) {

			page = new Page(clusteringIndex, 1);
			String pagePath = page.getPagePath(this.tableName);
			pagesPath.add(pagePath);
			page.insert(tuple);
			ranges.add(page.getRange());

			for (Map.Entry<String, String> column : indices.entrySet()) {
				Object key = tuple.getRecord(getIndexOfColumn(column.getKey()));
				BTreePlus tree = Serializer.deserializeFrom(column.getValue());

				tree.insert(key, page.getPageIndex());

				Serializer.serializeTo(tree, column.getValue());
			}

			Serializer.serializeTo(page, pagePath);
			Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
			return;

		} else {

			Object clusteringObject = tuple.getRecord(clusteringIndex);
			Range range = new Range(clusteringObject, clusteringObject, false);
			int index = Collections.binarySearch(ranges, range);

			if (index < 0)
				index = -index - 1;

			if (index == 0 || index == pagesPath.size())
				index = index == pagesPath.size() ? index - 1 : index;
			else {
				int current = index - 1;
				int next = index;

				if (ranges.get(next).inRange(clusteringObject) || ranges.get(current).isFull)
					index = next;
				else
					index = current;
			}

			Page p = Serializer.deserializeFrom(pagesPath.get(index));
			insertHelper(p, tuple, index);
		}

		System.out.println();

	}

	public int getIndexOfColumn(String columnName) {
		if (!columns.containsKey(columnName))
			return -1;

		int i = 0;
		for (String column : columns.keySet()) {
			if (column.equals(columnName))
				return i;

			i++;
		}
		return i;
	}

	public static int getIndexOfColumn(String columnName, Hashtable<String, String> columns) {
		if (!columns.containsKey(columnName))
			return -1;

		int i = 0;
		for (String column : columns.keySet()) {
			if (column.equals(columnName))
				return i;

			i++;
		}
		return i;
	}

	public static void updateIndex(Hashtable<String, String> indices, Vector<Tuple> deletedTuples,
			Object pindex, Hashtable<String, String> columns) throws DBAppException {
		if (deletedTuples.isEmpty())
			return;
		for (Map.Entry<String, String> entry : indices.entrySet()) {
			String name = entry.getKey();
			String bpath = indices.get(name);
			BTreePlus btree = Serializer.deserializeFrom(bpath);
			int index = getIndexOfColumn(name, columns);
			for (Tuple t : deletedTuples) {
				Object key = t.getRecord(index);
				btree.delete(key, pindex);
			}
			Serializer.serializeTo(btree, bpath);
		}
	}

	public void deleteWithClusteringKey(Hashtable<String, Object> records, Vector<String> pages,
			Vector<Range> rangesToDelete) throws DBAppException {
		Object o = records.get(clusteringColumn);
		Page page;
		int index = findPageMightContainTuple(o);
		if (index < 0)
			return;
		String path = pagesPath.get(index);
		Range oldRange = ranges.get(index);
		page = Serializer.deserializeFrom(path);
		int x = page.deleteHelper(records, this.columns, this.indices, true, this.clusteringColumn);

		deleteFromPage(x, index, page, oldRange, pages, rangesToDelete);
	}

	public void deleteFromPage(int noTuples, int i, Page page, Range oldRange, Vector<String> pages,
			Vector<Range> rangesToDelete) throws DBAppException {
		String path = page.getPagePath(tableName);
		if (noTuples == 0) {
			pages.add(path);
			rangesToDelete.add(ranges.get(i));
			File file = new File(path);
			if (file.exists()) {
				file.delete();
			}
		}

		else {
			Serializer.serializeTo(page, path);
			Range newRange = page.getRange();
			if (!oldRange.equals(newRange)) {
				ranges.setElementAt(newRange, i);
				Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
			}

		}

	}

	public void deleteTuple(Hashtable<String, Object> records) throws DBAppException, Exception {
		Page page;

		Vector<String> pages = new Vector<>();
		Vector<Range> rangesToDelete = new Vector<>();

		for (Map.Entry<String, Object> entry : records.entrySet()) {
			String name = entry.getKey();
			Object o = entry.getValue();
			String type = o.getClass().getName();
			if (!(columns.containsKey(name))) {
				throw new DBAppException("this column doesnt exist");
			}

			if (!(type.equalsIgnoreCase(columns.get(name)))) {
				throw new DBAppException("incompatible data types ");
			}
		}

		if (records.containsKey(clusteringColumn)) {
			deleteWithClusteringKey(records, pages, rangesToDelete);
			this.pagesPath.removeAll(pages);
			this.ranges.removeAll(rangesToDelete);
			Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
			return;
		}

		for (Map.Entry<String, Object> entry : records.entrySet()) {
			String name = entry.getKey();
			Object o = entry.getValue();
			if (indices.containsKey(name)) {
				String bpath = indices.get(name);
				BTreePlus btree = Serializer.deserializeFrom(bpath);
				Vector<Object> values = btree.search(o, false);
				for (int i = 0; i < values.size(); i++) {
					int y = (int) values.get(i);
					String path = Page.getPagePath(this.tableName, y);
					page = Serializer.deserializeFrom(path);
					Range oldRange = page.getRange();
					int RangeIndex = ranges.indexOf(oldRange);
					int x = page.deleteHelper(records, this.columns, this.indices, false, this.clusteringColumn);
					deleteFromPage(x, RangeIndex, page, oldRange, pages, rangesToDelete);
					this.pagesPath.removeAll(pages);
					this.ranges.removeAll(rangesToDelete);
					Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
				}

				return;
			}
		}

		for (int i = 0; i < pagesPath.size(); i++) {
			String path = pagesPath.get(i);
			Range oldRange = ranges.get(i);
			page = Serializer.deserializeFrom(path);
			int x = page.deleteHelper(records, this.columns, this.indices, false, this.clusteringColumn);
			deleteFromPage(x, i, page, oldRange, pages, rangesToDelete);
		}
		this.pagesPath.removeAll(pages);
		this.ranges.removeAll(rangesToDelete);
		Serializer.serializeTo(ranges, "Tables\\" + tableName + "\\ranges.class");
	}

	public void createIndex(String columnName, String indexName) throws DBAppException {
		String indexPath = "Tables\\" + tableName + "\\" + indexName + ".ser";
		indices.put(columnName, indexPath);
		BTreePlus tree = new BTreePlus();
		int colIndex = getIndexOfColumn(columnName);
		for (String path : pagesPath) {
			Page p = Serializer.deserializeFrom(path);
			int pIndex = p.getPageIndex();
			for (Tuple tuple : p.getTuples()) {

				tree.insert(tuple.getRecord(colIndex), pIndex);
			}
		}
		tree.print();
		Serializer.serializeTo(tree, indexPath);
	}

	public boolean columnIsIndexed(String columnName) {
		return indices.containsKey(columnName);
	}

	private void handleBothGreaterThans(boolean orEqual, Comparable value,
			int index, Range range, Vector<Tuple> result) throws DBAppException {
		if (range.inRange(value)) {
			if (!orEqual && range.equalsMax(value)) {
				for (int i = index + 1; i < getPagesPath().size(); i++) {
					Page page = Serializer.deserializeFrom(getPagesPath().elementAt(i));
					result.addAll(page.getTuples());
				}
			} else {
				for (int i = index; i < getPagesPath().size(); i++) {
					Page page = Serializer.deserializeFrom(getPagesPath().elementAt(i));
					if (i == index) {
						int tupleIndex = Collections.binarySearch(page.getTuples(), new Tuple(value));

						if (tupleIndex < 0)
							tupleIndex = -tupleIndex - 1;
						else if (!orEqual)
							tupleIndex++;

						if (tupleIndex == page.getTuples().size())
							continue;

						Vector<Tuple> tuples = page.getTuples();

						for (int j = tupleIndex; j < tuples.size(); j++) {
							result.add(tuples.get(j));
						}
					} else {
						result.addAll(page.getTuples());
					}

				}
			}
		} else {
			if (range.isLessThanEqualMin(value)) {
				for (int i = index; i < getPagesPath().size(); i++) {
					Page page = Serializer.deserializeFrom(getPagesPath().elementAt(i));
					result.addAll(page.getTuples());
				}
			}

		}

	}

	private void handleBothLessThans(boolean orEqual, Comparable value,
			int index, Range range, Vector<Tuple> result) throws DBAppException {

		if (range.inRange(value)) {
			if (!orEqual && range.isLessThanEqualMin(value))
				index--;
			if (index >= 0) {
				for (int i = 0; i <= index; i++) {
					Page page = Serializer.deserializeFrom(getPagesPath().elementAt(i));
					if (i == index) {
						int tupleIndex = Collections.binarySearch(page.getTuples(), new Tuple(value));

						if (!orEqual && tupleIndex >= 0)
							tupleIndex--;
						else if (tupleIndex < 0)
							tupleIndex = -tupleIndex - 1 - 1;

						if (tupleIndex == page.getTuples().size())
							tupleIndex--;

						Vector<Tuple> tuples = page.getTuples();

						for (int j = 0; j <= tupleIndex; j++) {
							result.add(tuples.get(j));
						}
					} else {
						result.addAll(page.getTuples());
					}
				}
			}
		} else {
			if (range.isLessThanEqualMin(value))
				index--;
			if (!(index < 0)) {
				for (int i = 0; i <= index; i++) {
					Page page = Serializer.deserializeFrom(getPagesPath().elementAt(i));
					result.addAll(page.getTuples());
				}
			}
		}
	}

	private void handleNotEqualCK(Comparable value, Vector<Tuple> result) throws DBAppException {
		for (String pagePath : getPagesPath()) {
			Page page = Serializer.deserializeFrom(pagePath);
			result.addAll(page.getTuples());
		}

		int toRemove = Collections.binarySearch(result, new Tuple(value));
		if (toRemove >= 0)
			result.remove(toRemove);
	}

	public void selectUnIndexedCK(Comparable value, int index, Range range,
			String operator, Vector<Tuple> result) throws DBAppException {
		switch (operator) {
			case "=":

				if (range.inRange(value)) {
					Page page = Serializer.deserializeFrom(getPagesPath().get(index));

					int tupleIndex = Collections.binarySearch(page.getTuples(), new Tuple(value));
					if (tupleIndex >= 0)
						result.add(page.getTuples().elementAt(tupleIndex));
				}
				break;

			case "!=":
				handleNotEqualCK(value, result);
				break;
			case ">":
				handleBothGreaterThans(false, value, index, range, result);
				break;
			case ">=":
				handleBothGreaterThans(true, value, index, range, result);
				break;
			case "<":
				handleBothLessThans(false, value, index, range, result);
				break;
			case "<=":
				handleBothLessThans(true, value, index, range, result);
				break;
		}

	}

	public void selectIndexedCol(Comparable value,
			String operator, BTreePlus tree, String columnName, Vector<Tuple> result) throws DBAppException {

		int columnIndex = getIndexOfColumn(columnName);
		Vector<Pair<Object, Object>> tempResult;
		Vector<String> paths = new Vector<>();
		switch (operator) {

			case "=":
				Vector<Object> temp = tree.search(value, false); // for the same key we might get the same page number
																	// aktar mn marra
				for (Object distinctValue : temp) {
					int i = (int) distinctValue;
					String path = Page.getPagePath(this.tableName, i);
					paths.add(path);		
				}
				
				selectUnIndexedCol(paths, value, columnName, "=", result);

				break;

			case "!=":
				if (columnName.equals(clusteringColumn))
					handleNotEqualCK(value, result);
				else
					for (String pagePath : pagesPath) {
						Page page = Serializer.deserializeFrom(pagePath);
						for (Tuple tuple : page.getTuples()) {
							if (!tuple.getRecord(columnIndex).equals(value))
								result.add(tuple);
						}
					}
				break;

			case ">":
			case ">=":
			case "<=":
			case "<":

				tempResult = tree.searchOperator(value, operator, false);
				Set<String> set = new HashSet<>();
				for (Pair pair : tempResult) {

					Vector<Integer> pageIndices = (Vector<Integer>) pair.getSecond(); // page1, page2, page3, page4
					for (int pageIndex : pageIndices)
						set.add(Page.getPagePath(this.tableName, pageIndex));
				}

				selectUnIndexedCol(new Vector<String>(set), value, columnName, operator, result);

		}

	}

	public static boolean satisfiesOperator(Object object, Object toCompare, String operator) throws DBAppException {
		int diff = ((Comparable) object).compareTo(toCompare);
		switch (operator) {
			case "=":
				return diff == 0;
			case "!=":
				return diff != 0;
			case ">=":
				return diff >= 0;
			case ">":
				return diff > 0;
			case "<=":
				return diff <= 0;
			case "<":
				return diff < 0;
		}

		throw new DBAppException("Wrong operator");
	}

	// Linear search all pages
	public void selectUnIndexedCol(Vector<String> paths, Comparable value, String columnName,
			String operator, Vector<Tuple> result) throws DBAppException {

		for (String pagePath : paths) {
			Page page = Serializer.deserializeFrom(pagePath);
			int columnIndex = getIndexOfColumn(columnName);
			for (Tuple tuple : page.getTuples()) {
				boolean satisfies = satisfiesOperator(tuple.getRecord(columnIndex), value, operator);
				if (satisfies)
					result.add(tuple);
			}
		}
	}

	public void update(String strClusteringKeyValue,
			Hashtable<String, Object> records) throws DBAppException {

		if (records.keySet().contains(clusteringColumn))
			throw new DBAppException("Clustering key cannot be updated");
		// check the inputed columns, their types, the new values types
		for (Map.Entry<String, Object> entry : records.entrySet()) {
			String columnName = entry.getKey();// column name
			Object newValue = entry.getValue();// the new value
			String type = newValue.getClass().getName();
			if (!(columns.containsKey(columnName))) {
				throw new DBAppException("this column doesnt exist");
			}

			if (!(type.equalsIgnoreCase(columns.get(columnName)))) {
				throw new DBAppException("incompatible data types ");
			}
		}

		// locate the page that might contain the PK we're given
		String clusteringType = columns.get(clusteringColumn);
		Object tcStrClusteringKeyValue = strClusteringKeyValue;
		if (clusteringType.equalsIgnoreCase("java.lang.double")) {
			tcStrClusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
		}
		if (clusteringType.equalsIgnoreCase("java.lang.integer")) {
			tcStrClusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
		}
		int index = findPageMightContainTuple(tcStrClusteringKeyValue);

		// if the pk doesnt exist, return
		if (index == -1) {
			return;
		}

		// get the page, deserialize, update and serialize
		String path = pagesPath.get(index);
		// deserializing the page that contains the tuple
		Page p = Serializer.deserializeFrom(path);
		Tuple tempTuple = new Tuple(tcStrClusteringKeyValue);
		int tupleIndex = Collections.binarySearch(p.getTuples(), tempTuple);

		// check all the hashtable keys and values, and update the table and
		// corresponding BTree if exists
		for (Map.Entry<String, Object> entry : records.entrySet()) { // pair feeh esm el table w el new value
			String columnName = entry.getKey(); // column name
			Object newValue = entry.getValue(); // the new value

			int entryIndex = getIndexOfColumn(columnName); // getting the index of the cell , use it inside the tuple

			if (tupleIndex >= 0) {
				Tuple tuple = p.getTuples().get(tupleIndex);
				Object oldvalue = tuple.getRecord(entryIndex);
				if (columnIsIndexed(columnName)) {
					String bpath = indices.get(columnName);
					BTreePlus btree = Serializer.deserializeFrom(bpath);
					btree.update(oldvalue, newValue, p.getPageIndex());
					Serializer.serializeTo(btree, bpath);
				}

				tuple.setRecord(newValue, entryIndex);
			}
		}
		Serializer.serializeTo(p, path);
		return;

	}

	public Vector<Tuple> selectFromTable(String columnName,
			String operator, Comparable value) throws DBAppException {
		Vector<Tuple> result = new Vector<>();

		if (pagesPath.size() == 0)
			return result;

		if (columnName.equals(clusteringColumn)) {
			int index = Collections.binarySearch(getRanges(), new Range(value, value, false));

			if (index < 0)
				index = -index - 1;

			if (index == getRanges().size())
				index--;

			Range range = getRanges().get(index);
			selectUnIndexedCK(value, index, range, operator, result);
		} else {
			if (columnIsIndexed(columnName)) {
				BTreePlus tree = Serializer.deserializeFrom(indices.get(columnName));
				selectIndexedCol(value, operator, tree, columnName, result);

			} else {
				selectUnIndexedCol(pagesPath, value, columnName, operator, result);
			}

		}

		return result;
	}
}