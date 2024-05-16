package engine;

import java.io.Serializable;

public class Range implements Serializable, Comparable<Range> {
    public Object min;
    public Object max;
    public boolean isFull;

    public Range(Object min, Object max, boolean isFull) {
        this.min = min;
        this.max = max;
        this.isFull = isFull;
    }

    public boolean inRange(Object value) {
        return isGreaterThanEqualMin(value) && isLessThanEqualMax(value);
    }

    @SuppressWarnings("unchecked")
    public boolean isGreaterThanEqualMax(Object value) {

        Comparable<Object> obj1 = (Comparable<Object>) value;
        Comparable<Object> obj2 = (Comparable<Object>) max;

        return obj1.compareTo(obj2) >= 0;
    }

    @SuppressWarnings("unchecked")
    public boolean isLessThanEqualMax(Object value) {
        Comparable<Object> obj1 = (Comparable<Object>) value;
        Comparable<Object> obj2 = (Comparable<Object>) max;

        return obj1.compareTo(obj2) <= 0;
    }

    @SuppressWarnings("unchecked")
    public boolean isGreaterThanEqualMin(Object value) {
        Comparable<Object> obj1 = (Comparable<Object>) value;
        Comparable<Object> obj2 = (Comparable<Object>) min;

        return obj1.compareTo(obj2) >= 0;
    }

    @SuppressWarnings("unchecked")
    public boolean isLessThanEqualMin(Object value) {
        Comparable<Object> obj1 = (Comparable<Object>) value;
        Comparable<Object> obj2 = (Comparable<Object>) min;

        return obj1.compareTo(obj2) <= 0;
    }

    @SuppressWarnings("unchecked")
    public boolean equalsMax(Object value) {
        Comparable<Object> obj1 = (Comparable<Object>) value;
        Comparable<Object> obj2 = (Comparable<Object>) max;

        return obj1.compareTo(obj2) == 0;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Range))
            return false;

        Range range = (Range) obj;
        return this.min.equals(range.min) && this.max.equals(range.max);
    }

    public int compareTo(Range other) {
        Object value = other.min;

        if (this.min.equals(other.min) && this.max.equals(other.max))
            return 0;

        if (this.inRange(value)) // 25 in range fe (24-30) fa -> (25-25), (24-30) hena -index-1 returns the page
                                 // to insert to
            return 1;
        if (this.isGreaterThanEqualMax(value)) // 25 w (10-24) fa -> (10-24), (25-25) (26-40)hane3mel insert in (10-24)
                                               // fa (-index-1) -1
            return -1;

        return 1; // 11 w (15-20) -> (11-11), (15-20) hena -index-1 returns the page to insert to
    }
}
