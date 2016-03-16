package norm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FindingAllSubSets<E> {
    private final List<E> set;
    private final int maximum;
    private int indexLHS;
    public FindingAllSubSets(List<E> originalList) {
        set = originalList;
        maximum = (1 << set.size());
        indexLHS = 0;
    }
    public boolean hasNext() {
        return indexLHS < maximum;
    }
    public List<E> next() {
        List<E> newSet = new ArrayList<E>();
        int check = 1;
        for (E element : set) {
            if ((indexLHS & check) != 0) {
                newSet.add(element);
            }
            check <<= 1;
        }
        ++indexLHS;
        return newSet;
    }


}