/**
 *
 */
package openr66.protocol.exception;

/**
 * Protocol exception on Packet
 *
 * @author frederic bregier
 */
public class OpenR66ProtocolPacketException extends OpenR66ProtocolException {

    /**
	 *
	 */
    private static final long serialVersionUID = 325267029289992117L;

    /**
	 *
	 */
    public OpenR66ProtocolPacketException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public OpenR66ProtocolPacketException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolPacketException(String arg0) {
        super(arg0);
    }

    /**
     * @param arg0
     */
    public OpenR66ProtocolPacketException(Throwable arg0) {
        super(arg0);
    }

}
