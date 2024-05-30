/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rc.soop.maintenance;

/**
 *
 * @author rcosco
 */
public class BreakException extends RuntimeException {

    public BreakException() {
        super();
    }
    public BreakException(String s) {
        super(s);
    }

    public BreakException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public BreakException(Throwable throwable) {
        super(throwable);
    }    
}
