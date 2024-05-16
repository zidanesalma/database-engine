package BTree;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Vector;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreePlus implements Serializable {
	private BTree tree;

	public BTreePlus() {
		tree = new BTree();
	}

	public void insert(Object key, Object value) {
		Comparable obj = (Comparable) key;
		Vector<Object> v = (Vector<Object>) tree.search(obj);

		if (v == null) {
			v = new Vector<Object>();
			tree.insert(obj, v);
		}

		v.add(value);
	}

	public void update(Object oldKey, Object newKey, Object value) {

		Vector<Object> newVector = (Vector<Object>) tree.search((Comparable) newKey);
		newVector.add(value);

		delete(oldKey, value);
	}

	public void delete(Object key, Object value) {
		Comparable obj = (Comparable) key;
		Vector<Object> v = (Vector<Object>) tree.search(obj);

		v.remove(value);
		if (v.isEmpty())
			tree.delete(obj);

		tree.print();
	}

	public Vector<Object> search(Object key) {
		return (Vector<Object>) tree.search((Comparable) key);
	}

	public Vector<Object> search(Object key, boolean getDuplicates) {
		Vector<Object> result = (Vector<Object>) tree.search((Comparable) key);

		if (getDuplicates)
			return result;

		return new Vector<>(new HashSet<>(result));
	}

	public Vector<Pair<Object, Object>> searchOperator(Comparable bound, String operator) {
		switch (operator) {
			case ">":
				return tree.searchStartingFrom(bound, false);
			case ">=":
				return tree.searchStartingFrom(bound, true);
			case "<=":
				return tree.searchTill(bound, true);
			case "<":
				return tree.searchTill(bound, false);
		}
		return null;
	}

	public Vector<Pair<Object, Object>> searchOperator(Comparable bound, String operator, boolean getDuplicates) {
		Vector<Pair<Object, Object>> result = searchOperator(bound, operator);
		if (getDuplicates)
			return result;

		return new Vector<>(new HashSet<>(result));
	}

	public void print() {
		tree.print();
	}
}