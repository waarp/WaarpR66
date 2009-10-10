/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Business part due to Query is already finished
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolBusinessQueryAlreadyFinishedException extends OpenR66ProtocolBusinessException {
    /**
     *
     */
    private static final long serialVersionUID = 1014687763768508552L;

    /**
	 *
	 */
    public OpenR66ProtocolBusinessQueryAlreadyFinishedException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolBusinessQueryAlreadyFinishedException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessQueryAlreadyFinishedException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolBusinessQueryAlreadyFinishedException(Throwable arg0) {
        super(arg0);
    }

}
