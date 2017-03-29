package org.waarp.openr66.protocol.localhandler.packet.json;

import org.junit.Test;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransferRequestJsonPacketTest extends JsonPacket {
    
    @Test
    public void testItIsDeserialized() {
        try {
            String json = "{\"requested\": \"server1\", \"@class\": \"org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket\", \"rulename\": \"send\", \"filename\": \"test.dat\"}";
            Object res = new ObjectMapper().readValue(json, TransferRequestJsonPacket.class);
            assertEquals(TransferRequestJsonPacket.class, res.getClass());
        } catch(Exception e) {
            fail("Got unexpected exception" + e);
        }
    }
    
    @Test
    public void testItIsDeserializedFromJsonPacket() {
        try {
            String json = "{\"requested\": \"server1\", \"@class\": \"org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket\", \"rulename\": \"send\", \"filename\": \"test.dat\"}";
            Object res = new ObjectMapper().readValue(json, JsonPacket.class);
            assertEquals(TransferRequestJsonPacket.class, res.getClass());
        } catch(Exception e) {
            fail("Got unexpected exception" + e);
        }
    }

    @Test
    public void testSetFileInformation() {
        TransferRequestJsonPacket packet = new TransferRequestJsonPacket();

        packet.setFileInformation("foo");
        assertEquals("The given value should have been set", "foo", packet.getFileInformation()); 
        
        packet.setFileInformation((String)null);
        assertEquals("null should be set as an empty string", "", packet.getFileInformation()); 
    }

    @Test
    public void testFileInformationShouldBeDeserialized() {
        System.out.println("in should be deserialized");
        try {
            String json = "{\"fileInformation\": \"foo\", \"@class\": \"org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket\"}";
            TransferRequestJsonPacket res = new ObjectMapper().readValue(json, TransferRequestJsonPacket.class);
            assertEquals("fileInformation should be deserialized",
                    "foo", res.getFileInformation());
        } catch(Exception e) {
            fail("Got unexpected exception" + e);
        }
    }

    @Test
    public void testFileInformationShouldNotBeNullAfterDeserialization() {
        System.out.println("in should not be null");
        try {
            String json = "{\"@class\": \"org.waarp.openr66.protocol.localhandler.packet.json.TransferRequestJsonPacket\"}";
            TransferRequestJsonPacket res = new ObjectMapper().readValue(json, TransferRequestJsonPacket.class);
            assertEquals("fileInformation should not be null",
                    "", res.getFileInformation());
        } catch(Exception e) {
            fail("Got unexpected exception" + e);
        }
    }
}
