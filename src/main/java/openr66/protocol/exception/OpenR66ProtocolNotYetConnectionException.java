/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol Exception when a connection is not yet possible but could be later on
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolNotYetConnectionException extends OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = -4985652825229717572L;

    /**
	 *
	 */
    public OpenR66ProtocolNotYetConnectionException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolNotYetConnectionException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNotYetConnectionException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNotYetConnectionException(Throwable arg0) {
        super(arg0);
    }

}
