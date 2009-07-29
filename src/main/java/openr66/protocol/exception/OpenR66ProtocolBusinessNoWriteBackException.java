/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part without any write back action
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessNoWriteBackException extends
        OpenR66Exception {

    /**
     *
     */
    private static final long serialVersionUID = -9088521827450885700L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessNoWriteBackException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessNoWriteBackException(String arg0,
            Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessNoWriteBackException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessNoWriteBackException(Throwable arg0) {
        super(arg0);
    }

}
