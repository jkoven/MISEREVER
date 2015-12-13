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
public class TermData implements Comparable {
    public String term;
    public Long freq;
    public Long totalDocFreq;
    public double tfIdf;
    public boolean tfIdfSet;
    public SearchCollector sc;
    public int group;

    public TermData(String term, long freq) {
        this.term = term;
        this.freq = freq;
        sc = null;
        group = 0;
    }
    
    
    @Override
    public int compareTo(Object p) throws ClassCastException{
        if (!(p instanceof TermData)){
            throw new ClassCastException("TermData Object Expected");
        }
        TermData another = (TermData) p;
        return freq.compareTo(another.freq);
    }
    
    @Override
    public boolean equals(Object p) throws ClassCastException{
        if (!(p instanceof TermData)){
            throw new ClassCastException("TermData Object Expected");
        }
        TermData another = (TermData) p;
        return term.equals(another.term);
    }
    
}
