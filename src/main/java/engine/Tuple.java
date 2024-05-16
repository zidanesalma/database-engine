package engine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Vector;
import java.util.stream.Collectors;

public class Tuple implements Serializable, Comparable<Tuple> {
    private Vector<Comparable<?>> records;
    
    private int clusteringIndex;

    public Tuple(Hashtable<String, Object> records, int clusteringIndex) {
        this.records = new Vector<>();
        this.clusteringIndex = clusteringIndex;
        for (Object record : records.values())
            this.records.add((Comparable<?>)record);
    }

    public Tuple(Object clusteringObject) {
        this.records = new Vector<>();
        this.clusteringIndex = 0;
        
        records.add((Comparable<?>)clusteringObject);
    }

    public Comparable<?> getRecord(int index) {
        return records.elementAt(index);
    }

    public void setRecord(Object record, int index) {
        records.setElementAt((Comparable<?>)record, index);
    }

    public String toString() {
        return records.stream().map(Object::toString).collect(Collectors.joining(","));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public int compareTo(Tuple tuple) {
        Comparable haga1 = (Comparable) this.getRecord(this.clusteringIndex);
        Comparable haga2 = (Comparable) tuple.getRecord(tuple.clusteringIndex);
        return haga1.compareTo(haga2);
    }

    public boolean equals(Object object) {
        if (!(object instanceof Tuple))
            return false;
        
        Tuple tuple = (Tuple)object;
        return tuple.clusteringIndex == this.clusteringIndex && this.compareTo(tuple) == 0;
    }

    public int hashCode() {
        return Objects.hash(records, clusteringIndex);
    }
}
