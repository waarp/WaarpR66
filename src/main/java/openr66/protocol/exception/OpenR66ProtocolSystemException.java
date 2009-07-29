/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception due to system error
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolSystemException extends OpenR66Exception {

    /**
	 *
	 */
    private static final long serialVersionUID = 586197904468892052L;

    /**
	 *
	 */
    public OpenR66ProtocolSystemException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolSystemException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolSystemException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolSystemException(Throwable arg0) {
        super(arg0);
    }

}
