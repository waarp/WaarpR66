/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol Exception when no connection is possible
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolNoConnectionException extends OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = -4525294313715038212L;

    /**
	 *
	 */
    public OpenR66ProtocolNoConnectionException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolNoConnectionException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNoConnectionException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNoConnectionException(Throwable arg0) {
        super(arg0);
    }

}
