/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

/**
 *
 * @author jkoven
 */
public class Pair<T, U> implements Comparable {

    public T key;
    public U value;

    public Pair(T k, U v) {
        this.key = k;
        this.value = v;
    }

    @Override
    public int compareTo(Object p) throws ClassCastException {
        if (!(p instanceof Pair)) {
            throw new ClassCastException("Pair Object Expected");
        }
        Pair another = (Pair) p;
        if (value instanceof Long && another.value instanceof Long) {
            return ((Long) value).compareTo((Long) another.value);
        } else if (value instanceof Integer && another.value instanceof Integer) {
            return ((Integer) value).compareTo((Integer) another.value);
        } else {
            return 0;
        }
    }
}
