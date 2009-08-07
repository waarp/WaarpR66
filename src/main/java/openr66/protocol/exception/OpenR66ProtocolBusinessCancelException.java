/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessCancelException extends OpenR66ProtocolBusinessException {

    /**
     *
     */
    private static final long serialVersionUID = 2339971663355797702L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessCancelException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessCancelException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessCancelException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessCancelException(Throwable arg0) {
        super(arg0);
    }

}
