package engine;

import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

public class Page implements Serializable {
    public static int maxRowCount;
    private final Vector<Tuple> tuples;
    private int clusteringIndex;
    private int pageIndex;

    public Page(int clusteringIndex, int pageIndex) {
        tuples = new Vector<>();
        this.clusteringIndex = clusteringIndex;
        this.pageIndex = pageIndex;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public Object getMin() {
        return tuples.getFirst().getRecord(clusteringIndex);
    }

    public Object getMax() {
        return tuples.getLast().getRecord(clusteringIndex);
    }

    public Range getRange() {
        return new Range(getMin(), getMax(), isFull());
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public Tuple deleteAndGetLastTuple() {
        return tuples.removeLast();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean shouldDelete(Hashtable<String, Object> records, Tuple p, Hashtable<String, String> columns) {

        for (Map.Entry<String, Object> entry : records.entrySet()) {
            int x = Table.getIndexOfColumn(entry.getKey(), columns);
            Object o = entry.getValue();
            if (!(((Comparable) p.getRecord(x)).compareTo(o) == 0)) {
                return false;

            }
        }
        return true;
    }

    public int deleteHelper(Hashtable<String, Object> records, Hashtable<String, String> columns,
            Hashtable<String, String> indices, boolean clustering, String clusteringname) throws DBAppException {
        Vector<Tuple> deletedTuples = new Vector<>();
        boolean flag = true;
        if (clustering == true) {
            Tuple t = new Tuple(records.get(clusteringname));
            int index = Collections.binarySearch(tuples, t);
            if (index < 0)
                return tuples.size();
            Tuple p = tuples.get(index);
            flag = shouldDelete(records, p, columns);
            if (flag == true) {
                tuples.remove(tuples.get(index));
                deletedTuples.add(t);

            }

        } else {
            for (int i = 0; i < tuples.size(); i++) {
                Tuple t = tuples.get(i);
                flag = shouldDelete(records, t, columns);
                if (flag == true) {
                    deletedTuples.add(t);

                }
            }
            tuples.removeAll(deletedTuples);
        }
        Table.updateIndex(indices, deletedTuples, this.pageIndex, columns);
        return tuples.size();
    }

    public String getPagePath(String tableName) {
        return "Tables\\" + tableName + "\\" + tableName + pageIndex + ".class";
    }

    public static String getPagePath(String tableName, int pageIndex) {
        return "Tables\\" + tableName + "\\" + tableName + pageIndex + ".class";
    }

    public void insert(Tuple t) throws DBAppException {
        int index = Collections.binarySearch(tuples, t);
        if (index >= 0)
            throw new DBAppException("Cannot insert duplicate key");

        int insertionPoint = -index - 1;
        tuples.add(insertionPoint, t);
    }

    public boolean isFull() {
        return tuples.size() == maxRowCount;
    }

    public boolean maximumExceeded() {
        return tuples.size() > maxRowCount;
    }

    public String toString() {
        return tuples.stream().map(Object::toString).collect(Collectors.joining(","));
    }
}
