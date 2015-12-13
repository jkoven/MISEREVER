/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miserever;

import java.util.BitSet;

/**
 *
 * @author jkoven
 */
public class ScTermData implements Comparable {
    public String term;
    public Long freq;
    public Long totalDocFreq;
    public Double tfIdf;
    public boolean tfIdfSet;
    public SearchCollector sc;
    public BitSet emails;
    public int group;

    public ScTermData(String term, long freq) {
        this.term = term;
        this.freq = freq;
        this.totalDocFreq = 1L;
        this.tfIdfSet = false;
        this.emails = new BitSet();
        sc = null;
        group = 0;
    }
    public ScTermData(String term, long freq, int e) {
        this.term = term;
        this.freq = freq;
        this.totalDocFreq = 1L;
        this.tfIdfSet = false;
        this.emails = new BitSet();
        emails.set(e);
        sc = null;
    }
    
    
    @Override
    public int compareTo(Object p) throws ClassCastException{
        if (!(p instanceof ScTermData)){
            throw new ClassCastException("TermData Object Expected");
        }
        ScTermData another = (ScTermData) p;
        return freq.compareTo(another.freq);
    }
    
    @Override
    public boolean equals(Object p) throws ClassCastException{
        if (!(p instanceof ScTermData)){
            throw new ClassCastException("TermData Object Expected");
        }
        TermData another = (TermData) p;
        return term.equals(another.term);
    }
    
}
