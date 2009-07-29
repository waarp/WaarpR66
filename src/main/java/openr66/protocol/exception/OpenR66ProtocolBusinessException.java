/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part
 * 
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessException extends OpenR66Exception {

    /**
	 *
	 */
    private static final long serialVersionUID = -7827259682529953206L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessException(Throwable arg0) {
        super(arg0);
    }

}
