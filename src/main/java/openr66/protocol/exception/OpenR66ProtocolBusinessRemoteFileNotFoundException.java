/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part telling that remote host did not find the file.
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessRemoteFileNotFoundException extends OpenR66ProtocolBusinessException {

    /**
     *
     */
    private static final long serialVersionUID = -1515420982161281552L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessRemoteFileNotFoundException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessRemoteFileNotFoundException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessRemoteFileNotFoundException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessRemoteFileNotFoundException(Throwable arg0) {
        super(arg0);
    }

}
