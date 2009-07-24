/**
 *
 */
package openr66.protocol.exception;

/**
 * Mother class of All OpenR66 Exceptions
 *
 * @author frederic bregier
 */
@SuppressWarnings("serial")
public abstract class OpenR66Exception extends Exception {

    /**
	 *
	 */
    public OpenR66Exception() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66Exception(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66Exception(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66Exception(Throwable arg0) {
        super(arg0);
    }

}
