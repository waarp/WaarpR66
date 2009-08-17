/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol Exception when no SSL is supported
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolNoSslException extends OpenR66Exception {
    /**
     *
     */
    private static final long serialVersionUID = 765327612922240252L;

    /**
	 *
	 */
    public OpenR66ProtocolNoSslException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolNoSslException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNoSslException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNoSslException(Throwable arg0) {
        super(arg0);
    }

}
