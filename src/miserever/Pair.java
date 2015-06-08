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
public class Pair<T, U> {

    public T key;
    public U value;

    public Pair(T k, U v) {
        this.key = k;
        this.value = v;
    }    
}
