/**
 *
 */
package openr66.protocol.exception;

/**
 * Mother class of Protocol Exception
 *
 * @author frederic bregier
 */
@SuppressWarnings("serial")
public abstract class OpenR66ProtocolException extends Exception {

    /**
	 *
	 */
    public OpenR66ProtocolException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolException(Throwable arg0) {
        super(arg0);
    }

}
