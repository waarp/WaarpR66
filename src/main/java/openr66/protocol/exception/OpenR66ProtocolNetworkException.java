/**
 * 
 */
package openr66.protocol.exception;

/**
 * Protocol exception due to a network error
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolNetworkException extends OpenR66ProtocolException {
    /**
	 * 
	 */
    private static final long serialVersionUID = -623368703701931176L;

    /**
	 * 
	 */
    public OpenR66ProtocolNetworkException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolNetworkException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNetworkException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNetworkException(Throwable arg0) {
        super(arg0);
    }

}
