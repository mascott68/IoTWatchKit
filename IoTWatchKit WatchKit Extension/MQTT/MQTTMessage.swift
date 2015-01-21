//
//  MQTTMessage.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 05/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

enum MQTTMessageType: UInt8 {
    case MQTTConnect = 1,
    MQTTConnack = 2,
    MQTTPublish = 3,
    MQTTPuback = 4,
    MQTTPubrec = 5,
    MQTTPubrel = 6,
    MQTTPubcomp = 7,
    MQTTSubscribe = 8,
    MQTTSuback = 9,
    MQTTUnsubscribe = 10,
    MQTTUnsuback = 11,
    MQTTPingreq = 12,
    MQTTPingresp = 13,
    MQTTDisconnect = 14
}

extension NSMutableData {
    
    internal func appendByte(var byte: UInt8) {
        self.appendBytes(&byte, length: 1)
    }
    
    internal func appendUInt16BigEndian(var val: UInt16) {
        self.appendByte(UInt8(val / 256))
        self.appendByte(UInt8(val % 256))
    }
    
    internal func appendMQTTString(string: String) {
        var buf: Array<UInt8> = Array<UInt8>(count: 2, repeatedValue: 0)
        let utf8String: UnsafePointer<Int8> = (string as NSString).UTF8String
        var strLen: CInt = CInt(strlen(utf8String))
        
        buf[0] = UInt8(strLen / 256)
        buf[1] = UInt8(strLen % 256)
        self.appendBytes(&buf, length: 2)
        self.appendBytes(utf8String, length: Int(strLen))
    }
}


class MQTTMessage: NSObject {

// MARK: Message Properties
    
    var type: UInt8 = UInt8()
    var qos: UInt8 = UInt8()
    var retainFlag: Bool = Bool()
    var isDuplicate: Bool = Bool()
    var data: NSData?
    
 
// MARK: Instance Methods
        
    class func connectMessageWithClientId(clientId: String, userName: String, password: String, keepAlive: Int, cleanSessionFlag: Bool) -> MQTTMessage {
        
        var flags: UInt8 = 0x00
        
        if (cleanSessionFlag) {
            flags |= 0x02
        }
        if ((userName as NSString).length > 0) {
            flags |= 0x80
            if ((password as NSString).length > 0) {
                flags |= 0x40
            }
        }
        
        var data: NSMutableData = NSMutableData()
        data.appendMQTTString("MQTT")
        data.appendByte(4)
        data.appendByte(flags)
        data.appendUInt16BigEndian(UInt16(keepAlive))
        data.appendMQTTString(clientId)
        
        if ((userName as NSString).length > 0) {
            data.appendMQTTString(userName)
            if ((password as NSString).length > 0) {
                data.appendMQTTString(password)
            }
        }
        //println(data)

        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTConnect.rawValue, aData: data)
        return msg
    }

    class func connectMessageWithClientId(clientId: String, userName: String, password: String, keepAlive: Int, cleanSessionFlag: Bool, willTopic: String, willMsg: NSData, willQoS: UInt8, willRetainFlag: Bool) -> MQTTMessage {
    
        var flags: UInt8 = 0x04 | (willQoS << 4 & 0x18)
        
        if (willRetainFlag) {
            flags |= 0x20
        }
        if (cleanSessionFlag) {
            flags |= 0x02
        }
        if ((userName as NSString).length > 0) {
            flags |= 0x80;
            if ((password as NSString).length > 0) {
                flags |= 0x40;
            }
        }
        
        var data: NSMutableData = NSMutableData()
        data.appendMQTTString("MQTT")
        data.appendByte(4)
        data.appendByte(flags)
        data.appendUInt16BigEndian(UInt16(keepAlive))
        data.appendMQTTString(clientId)
        data.appendMQTTString(willTopic)
        data.appendUInt16BigEndian(UInt16(willMsg.length))
        data.appendData(willMsg)
        
        if ((userName as NSString).length > 0) {
            data.appendMQTTString(userName)
            if ((password as NSString).length > 0) {
                data.appendMQTTString(password)
            }
        }

        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTConnect.rawValue, aData: data)
        return msg
    }

    class func pingreqMessage () -> MQTTMessage {
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPingreq.rawValue)
        return msg
    }

    class func subscribeMessageWithMessageId (msgId: UInt16, topic: String, qos: UInt8) -> MQTTMessage {
        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        data.appendMQTTString(topic)
        data.appendByte(qos)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTSubscribe.rawValue, aQos: 1, aData: data)
        return msg
    }

    class func unsubscribeMessageWithMessageId (msgId: UInt16, topic: String) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        data.appendMQTTString(topic)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTUnsubscribe.rawValue, aQos: 1, aData: data)
        return msg
    }
    
    class func publishMessageWithData (payload: NSData, topic: String, retainFlag: Bool) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendMQTTString(topic)
        data.appendData(payload)

        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPublish.rawValue, aQos: 0, aRetainFlag: retainFlag, aDupFlag: false, aData: data)
        return msg
    }

    class func publishMessageWithData (payload: NSData, topic: String, qosLevel: UInt8, msgId: UInt16, retainFlag: Bool, dupFlag: Bool) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendMQTTString(topic)
        data.appendUInt16BigEndian(msgId)
        data.appendData(payload)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPublish.rawValue, aQos: qosLevel, aRetainFlag: retainFlag, aDupFlag: dupFlag, aData: data)
        return msg
    }
    
    class func pubackMessageWithMessageId (msgId: UInt16) -> MQTTMessage {

        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPuback.rawValue, aData: data)
        return msg
    }
    
    class func pubrecMessageWithMessageId (msgId: UInt16) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPubrec.rawValue, aData: data)
        return msg
    }
    
    class func pubrelMessageWithMessageId (msgId: UInt16) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPubrel.rawValue, aData: data)
        return msg
    }

    class func pubcompMessageWithMessageId (msgId: UInt16) -> MQTTMessage {
        
        var data: NSMutableData = NSMutableData()
        data.appendUInt16BigEndian(msgId)
        
        var msg: MQTTMessage =  MQTTMessage(aType: MQTTMessageType.MQTTPubcomp.rawValue, aData: data)
        return msg
    }

    init(aType: UInt8) {
        type = aType
        self.data = nil
    }

    init(aType: UInt8, aData: NSData) {
        type = aType
        self.data = aData
    }
    
    init(aType: UInt8, aQos: UInt8, aData: NSData) {
        type = aType
        qos = aQos
        self.data = aData
    }
    
    init(aType: UInt8, aQos: UInt8, aRetainFlag: Bool, aDupFlag: Bool, aData: NSData) {
        type = aType
        qos = aQos
        retainFlag = aRetainFlag
        isDuplicate = aDupFlag
        self.data = aData

    }
    
    func setDupFlag () {
        isDuplicate = true
    } 

}