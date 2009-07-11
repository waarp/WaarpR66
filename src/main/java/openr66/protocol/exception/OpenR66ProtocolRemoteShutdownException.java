/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol Exception to enable Shutdown
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolRemoteShutdownException extends OpenR66ProtocolException {


    /**
     *
     */
    private static final long serialVersionUID = 5871418368412513994L;

    /**
	 *
	 */
    public OpenR66ProtocolRemoteShutdownException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolRemoteShutdownException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolRemoteShutdownException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolRemoteShutdownException(Throwable arg0) {
        super(arg0);
    }

}
