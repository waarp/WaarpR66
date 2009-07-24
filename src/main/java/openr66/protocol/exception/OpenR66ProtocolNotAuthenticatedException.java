/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception due to no valid authentication
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolNotAuthenticatedException extends
        OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = 5671796883262590190L;

    /**
	 *
	 */
    public OpenR66ProtocolNotAuthenticatedException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolNotAuthenticatedException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNotAuthenticatedException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolNotAuthenticatedException(Throwable arg0) {
        super(arg0);
    }

}
