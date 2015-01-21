//
//  MQTTSession.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 05/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

enum MQTTSessionStatus {
    case Created
    case Connecting
    case Connected
    case Error
}

enum MQTTSessionEvent {
    case Connected
    case ConnectionRefused
    case ConnectionClosed
    case ConnectionError
    case ProtocolError
}

protocol MQTTSessionDelegate {
    func session(session: MQTTSession, handleEvent: MQTTSessionEvent)
    func session(session: MQTTSession, newMessage: NSData, onTopic: String)
}

class MQTTSession: NSObject, MQTTDecoderDelegate, MQTTEncoderDelegate {
    
    var status: MQTTSessionStatus?
    var clientId: String = String()
    var keepAliveInterval: UInt16 = UInt16()
    var cleanSessionFlag: Bool = Bool()
    var connectMessage: MQTTMessage?
    var runLoop: NSRunLoop = NSRunLoop()
    var runLoopMode: String = String()
    var timer: NSTimer?
    var idleTimer: Int = Int()
    var encoder: MQTTEncoder?
    var decoder: MQTTDecoder?
    var txMsgId: UInt16 = UInt16()
    var txFlows: NSMutableDictionary = NSMutableDictionary()
    var rxFlows: NSMutableDictionary = NSMutableDictionary()
    var ticks: CUnsignedInt = CUnsignedInt()
    
    private var queue: NSMutableArray?
    private var timerRing: NSMutableArray?

    var delegate: MQTTSessionDelegate?
    
    typealias connectionHandlerClosure = (event: MQTTSessionEvent) -> Void
    typealias messageHandlerClosure = (message: NSData, topic: String) -> Void
    
    var connectionHadler:connectionHandlerClosure?
    var messageHandler:messageHandlerClosure?
    

//MARK: INIT
    
    convenience init(aClientId: String) {
        self.init(
            aClientId: aClientId,
            aUserName: "",
            aPassword: ""
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String) {
        self.init(
            aClientId: aClientId,
            aUserName: aUserName,
            aPassword: aPassword,
            aKeepAliveInterval: 60,
            aCleanSessionFlag: true
        )
    }
    
    convenience init(aClientId: String, aRunLoop: NSRunLoop, aRunLoopMode: String) {
        self.init(
            aClientId: aClientId,
            aUserName: "",
            aPassword: "",
            aRunLoop: aRunLoop,
            aRunLoopMode: aRunLoopMode
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String, aRunLoop: NSRunLoop, aRunLoopMode: String) {
        self.init(
            aClientId: aClientId,
            aUserName: aUserName,
            aPassword: aPassword,
            aKeepAliveInterval: 60,
            aCleanSessionFlag: true,
            aRunLoop: aRunLoop,
            aRunLoopMode: aRunLoopMode
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String, aKeepAliveInterval: UInt16, aCleanSessionFlag: Bool) {
        self.init(
            aClientId: aClientId,
            aUserName: aUserName,
            aPassword: aPassword,
            aKeepAliveInterval: aKeepAliveInterval,
            aCleanSessionFlag: aCleanSessionFlag,
            aRunLoop: NSRunLoop.currentRunLoop(),
            aRunLoopMode: NSDefaultRunLoopMode
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String, aKeepAliveInterval: UInt16, aCleanSessionFlag: Bool, aRunLoop: NSRunLoop, aRunLoopMode: String) {
        
        var msg: MQTTMessage = MQTTMessage.connectMessageWithClientId(
            aClientId,
            userName: aUserName,
            password: aPassword,
            keepAlive: Int(aKeepAliveInterval),
            cleanSessionFlag: aCleanSessionFlag
        )
        
        self.init(
            aClientId: aClientId,
            aKeepAliveInterval: aKeepAliveInterval,
            aConnectMessage: msg,
            aRunLoop: aRunLoop,
            aRunLoopMode: aRunLoopMode
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String, aKeepAliveInterval: UInt16, aCleanSessionFlag: Bool, willTopic: String, willMsg: NSData, willQoS: UInt8, willRetainFlag: Bool) {
        self.init(
            aClientId: aClientId,
            aUserName: aUserName,
            aPassword: aPassword,
            aKeepAliveInterval: aKeepAliveInterval,
            aCleanSessionFlag: aCleanSessionFlag,
            willTopic: willTopic,
            willMsg: willMsg,
            willQoS: willQoS,
            willRetainFlag: willRetainFlag,
            aRunLoop: NSRunLoop.currentRunLoop(),
            aRunLoopMode: NSDefaultRunLoopMode
        )
    }
    
    convenience init(aClientId: String, aUserName: String, aPassword: String, aKeepAliveInterval: UInt16, aCleanSessionFlag: Bool, willTopic: String, willMsg: NSData, willQoS: UInt8, willRetainFlag: Bool, aRunLoop: NSRunLoop, aRunLoopMode: String) {
        
        var msg: MQTTMessage = MQTTMessage.connectMessageWithClientId(
            aClientId,
            userName: aUserName,
            password: aPassword,
            keepAlive: Int(aKeepAliveInterval),
            cleanSessionFlag: aCleanSessionFlag,
            willTopic: willTopic,
            willMsg: willMsg,
            willQoS: willQoS,
            willRetainFlag: willRetainFlag
        )
        
        self.init(
            aClientId: aClientId,
            aKeepAliveInterval: aKeepAliveInterval,
            aConnectMessage: msg,
            aRunLoop: aRunLoop,
            aRunLoopMode: aRunLoopMode
        )
    }
    
    init(aClientId: String, aKeepAliveInterval: UInt16, aConnectMessage: MQTTMessage, aRunLoop: NSRunLoop, aRunLoopMode: String) {
        
        clientId = aClientId
        keepAliveInterval = aKeepAliveInterval
        connectMessage = aConnectMessage
        runLoop = aRunLoop
        runLoopMode = aRunLoopMode

        self.queue = NSMutableArray()
        txMsgId = 1
        txFlows = NSMutableDictionary()
        rxFlows = NSMutableDictionary()
        self.timerRing = NSMutableArray(capacity: 60)
        
        for index in 0...60 {
            self.timerRing?.addObject(NSMutableSet())
        }
        ticks = 0
    }
    
    deinit {
        encoder?.close()
        encoder = nil
        decoder?.close()
        decoder = nil
        if (timer != nil) {
            timer?.invalidate()
            timer = nil;
        }
    }
    
    func close() {
        encoder?.close()
        decoder?.close()
        encoder = nil
        decoder = nil
        if (timer != nil) {
            timer?.invalidate()
            timer = nil;
        }
        self.error(MQTTSessionEvent.ConnectionClosed)
    }

//MARK: Connection Management
    
    func connectToHost(ip: String, port: UInt32) {
        self.connectToHost(ip, port: port, usingSSL: false)
    }
    
    func connectToHost(ip: String, port: UInt32, usingSSL: Bool) {
        
        status = MQTTSessionStatus.Created
        var readStream:  Unmanaged<CFReadStreamRef>?
        var writeStream: Unmanaged<CFWriteStreamRef>?
        
        CFStreamCreatePairWithSocketToHost(nil, ip, port, &readStream, &writeStream)
        
        if (usingSSL) {
            var sslSettings = Dictionary<NSObject, NSObject>()
            sslSettings[kCFStreamSSLLevel] = kCFStreamSocketSecurityLevelNegotiatedSSL
            sslSettings[kCFStreamSSLPeerName] = kCFNull
            
            CFReadStreamSetProperty(readStream?.takeUnretainedValue(), kCFStreamPropertySSLSettings, sslSettings)
            CFWriteStreamSetProperty(writeStream?.takeUnretainedValue(), kCFStreamPropertySSLSettings, sslSettings)
        }
        
        encoder = MQTTEncoder(aStream: writeStream!.takeUnretainedValue(), aRunLoop: runLoop, aMode: runLoopMode)
        decoder = MQTTDecoder(aStream: readStream!.takeUnretainedValue(), aRunLoop: runLoop, aMode: runLoopMode)

        encoder?.delegate = self
        decoder?.delegate = self

        encoder?.open()
        decoder?.open()
    }

    func connectToHost (ip: String, port: UInt32, connHandler: connectionHandlerClosure, messHandler: messageHandlerClosure) {
        self.connectToHost(ip, port: port, usingSSL: false, connHandler: connHandler, messHandler: messHandler)

    }

    func connectToHost (ip: String, port: UInt32, usingSSL: Bool, connHandler: connectionHandlerClosure, messHandler: messageHandlerClosure) {
        connectionHadler = connHandler
        messageHandler = messHandler
        
        self.connectToHost(ip, port: port, usingSSL: usingSSL)
    }
    
//MARK: Subscription Management
    
    func subscribeTopic (topic: String) {
        self.subscribeToTopic(topic, qosLevel: 0)
    }
    
    func subscribeToTopic (topic: String, qosLevel: UInt8) {
        self.send(MQTTMessage.subscribeMessageWithMessageId(self.nextMsgId(), topic: topic, qos: qosLevel))
    }
    
    func unsubscribeTopic (topic: String) {
        self.send(MQTTMessage.unsubscribeMessageWithMessageId(self.nextMsgId(), topic: topic))
    }
    
    func publishData (data: NSData, topic: String) {
        self.publishDataAtMostOnce(data, topic: topic)
    }
    
    func publishDataAtMostOnce (data: NSData, topic: String) {
        self.publishDataAtMostOnce(data, topic: topic, retainFlag: false)
    }
    
    func publishDataAtMostOnce (data: NSData, topic: String, retainFlag: Bool) {
        self.send(MQTTMessage.publishMessageWithData(data, topic: topic, retainFlag: retainFlag))
    }

    func publishDataAtLeastOnce (data: NSData, topic: String) {
        self.publishDataAtLeastOnce(data, topic: topic, retainFlag: false)
    }
    
    func publishDataAtLeastOnce (data: NSData, topic: String, retainFlag: Bool) {
        
        var msgId: UInt16 = self.nextMsgId()
        
        var msg: MQTTMessage = MQTTMessage.publishMessageWithData(
            data,
            topic: topic,
            qosLevel: 1,
            msgId: msgId,
            retainFlag: retainFlag,
            dupFlag: false
        )
        
        var flow: MQttTxFlow = MQttTxFlow.flowWithMsg(msg, deadline: (ticks + 60))
        txFlows.setObject(flow, forKey: NSNumber(unsignedInt: UInt32(msgId)))
        self.timerRing?.objectAtIndex(Int(flow.deadline) % 60).addObject(NSNumber(unsignedInt: UInt32(msgId)))
        self.send(msg)
    }

    func publishDataExactlyOnce (data: NSData, topic: String) {
        self.publishDataExactlyOnce(data, topic: topic, retainFlag: false)
    }

    func publishDataExactlyOnce (data: NSData, topic: String, retainFlag: Bool) {
        
        var msgId: UInt16 = self.nextMsgId()
        
        var msg: MQTTMessage = MQTTMessage.publishMessageWithData(
            data,
            topic: topic,
            qosLevel: 2,
            msgId: msgId,
            retainFlag: retainFlag,
            dupFlag: false
        )
        
        var flow: MQttTxFlow = MQttTxFlow.flowWithMsg(msg, deadline: (ticks + 60))
        txFlows.setObject(flow, forKey: NSNumber(unsignedInt: UInt32(msgId)))
        self.timerRing?.objectAtIndex(Int(flow.deadline) % 60).addObject(NSNumber(unsignedInt: UInt32(msgId)))
        self.send(msg)
    }
    
    func publishJson (payload: AnyObject, theTopic: String) {
        var error: NSError?
        let data: NSData = NSJSONSerialization.dataWithJSONObject(payload, options: NSJSONWritingOptions(0), error: &error)!

        if (error != nil) {
            println("Error creating JSON: \(error?.description)")
            return
        }
        self.publishData(data, topic: theTopic)

    }

    func timerHandler (theTimer: NSTimer) {
        
        idleTimer++
        if (idleTimer >= Int(keepAliveInterval)) {
            if (encoder?.status == MQTTEncoderStatus.Ready) {
                //println("sending PINGREQ")
                encoder?.encodeMessage(MQTTMessage.pingreqMessage())
                idleTimer = 0
            }
        }
        ticks++
        var e: NSEnumerator! = self.timerRing?.objectAtIndex(Int(ticks) % 60).objectEnumerator()

        while let msgId: AnyObject = e.nextObject() {
            let flow: MQttTxFlow = txFlows.objectForKey(msgId)! as MQttTxFlow
            let msg: MQTTMessage = flow.msg
            flow.deadline = ticks + 60
            msg.setDupFlag()
            self.send(msg)
        
        }
    }
    
    func encoder (sender: MQTTEncoder, handleEvent: MQTTEncoderEvent) {
        //println("encoder (sender: MQTTEncoder, handleEvent: MQTTEncoderEvent)")
        if(sender == encoder) {
            switch handleEvent {
            case MQTTEncoderEvent.Ready:
                switch (status!) {
                case MQTTSessionStatus.Created:
                    //println("Encoder has been created. Sending Auth Message")
                    sender.encodeMessage(connectMessage!)
                    status = MQTTSessionStatus.Connecting
                    break
                case MQTTSessionStatus.Connecting:
                    break
                case MQTTSessionStatus.Connected:
                    if (self.queue?.count > 0) {
                        let msg: MQTTMessage = self.queue?[0] as MQTTMessage
                        self.queue?.removeObjectAtIndex(0)
                        encoder?.encodeMessage(msg)
                    }
                    break
                case MQTTSessionStatus.Error:
                    break
                }
                break
            case MQTTEncoderEvent.ErrorOccurred:
                self.error(MQTTSessionEvent.ConnectionError)
                break
            }
        }
    }

    func decoder (sender: MQTTDecoder, handleEvent: MQTTDecoderEvent) {
        //println("decoder (sender: MQTTDecoder, handleEvent: MQTTDecoderEvent)")
        if(sender == decoder) {
            var event: MQTTSessionEvent
            switch (handleEvent) {
            case MQTTDecoderEvent.ConnectionClosed:
                event = MQTTSessionEvent.ConnectionError
                break
            case MQTTDecoderEvent.ConnectionError:
                event = MQTTSessionEvent.ConnectionError
                break
            case MQTTDecoderEvent.ProtocolError:
                event = MQTTSessionEvent.ProtocolError
                break
            }
            self.error(event)
        }
    }
    
    func decoder (sender: MQTTDecoder, msg: MQTTMessage) {
        //println("decoder (sender: MQTTDecoder, msg: MQTTMessage)")
        if(sender == decoder){
            switch (status!) {
            case MQTTSessionStatus.Connecting:
                switch (msg.type) {
                case MQTTMessageType.MQTTConnack.rawValue:
                    if (msg.data?.length != 2) {
                        self.error(MQTTSessionEvent.ProtocolError)
                    } else {
                        var bytes = UnsafePointer<UInt8>(msg.data!.bytes)
                        if (bytes[1] == 0) {
                            status = MQTTSessionStatus.Connected
                            timer = NSTimer(fireDate: NSDate(timeIntervalSinceNow: 1.0), interval: 1.0, target: self, selector: "timerHandler:", userInfo: nil, repeats: true)
                            if let closure = connectionHadler {
                                closure(event: MQTTSessionEvent.Connected)
                            }
                            delegate?.session(self, handleEvent: MQTTSessionEvent.Connected)
                            runLoop.addTimer(timer!, forMode: runLoopMode)
                        }
                        else {
                            self.error(MQTTSessionEvent.ConnectionRefused)
                        }
                    }
                    break
                default:
                    self.error(MQTTSessionEvent.ProtocolError)
                    break
                }
                break
            case MQTTSessionStatus.Connected:
                self.newMessage(msg)
                break
            default:
                break
            }
        }
    }

    func newMessage (msg: MQTTMessage) {
        switch (msg.type) {
        case MQTTMessageType.MQTTPublish.rawValue:
            self.handlePublish(msg)
            break
        case MQTTMessageType.MQTTPuback.rawValue:
            self.handlePuback(msg)
            break
        case MQTTMessageType.MQTTPubrec.rawValue:
            self.handlePubrec(msg)
            break
        case MQTTMessageType.MQTTPubrel.rawValue:
            self.handlePubrel(msg)
            break
        case MQTTMessageType.MQTTPubcomp.rawValue:
            self.handlePubcomp(msg)
            break
        default:
            return
        }
    }

    func handlePublish (msg: MQTTMessage) {
        var data: NSData = msg.data!
        if (data.length < 2) {
            return
        }
        var bytes = UnsafePointer<UInt8>(msg.data!.bytes)
        let topicLength = 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1])
        if (data.length < 2 + Int(topicLength)) {
            return
        }
        let topicData: NSData = data.subdataWithRange(NSMakeRange(2, Int(topicLength)))
        let topic: String =  NSString(data: topicData, encoding: NSUTF8StringEncoding)!
        let range: NSRange = NSMakeRange(2 + Int(topicLength), data.length - Int(topicLength) - 2)
        data = data.subdataWithRange(range)
        
        if (msg.qos == 0) {
            delegate?.session(self, newMessage: data, onTopic: topic)
            if let closure = messageHandler {
                closure(message: data, topic: topic)
            }
        } else {
            bytes = UnsafePointer<UInt8>(data.bytes)
            var msgId: NSNumber = NSNumber(unsignedInt: 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1]))
            if (msgId == 0) {
                return
            }
            data = data.subdataWithRange(NSMakeRange(2, data.length - 2))
            
            if (msg.qos == 1) {
                delegate?.session(self, newMessage: data, onTopic: topic)
                if let closure = messageHandler {
                    closure(message: data, topic: topic)
                }
                self.send(MQTTMessage.pubackMessageWithMessageId(msgId.unsignedShortValue))
            }
            else {
                let dict: Dictionary = ["data": data, "topic": topic]
                rxFlows.setObject(dict, forKey: NSNumber(unsignedInt: msgId.unsignedIntValue))
                self.send(MQTTMessage.pubrecMessageWithMessageId(msgId.unsignedShortValue))
            }
        }
}

    
    func handlePuback (msg: MQTTMessage) {
        if (msg.data?.length != 2) {
            return
        }
        let bytes = UnsafePointer<UInt8>(msg.data!.bytes)
        let msgId: NSNumber = NSNumber(unsignedInt: 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1]))
        if (msgId.unsignedIntValue == 0) {
            return
        }
        var flow: MQttTxFlow? = txFlows.objectForKey(msgId) as? MQttTxFlow
        if (flow == nil) {
            return
        }
        if (flow?.msg.type != MQTTMessageType.MQTTPublish.rawValue || flow?.msg.qos != 1) {
            return
        }
        self.timerRing?.objectAtIndex(Int(flow!.deadline) % 60).removeObject(msgId)
        txFlows.removeObjectForKey(msgId)
    }

    func handlePubrec (msg: MQTTMessage) {
        if (msg.data?.length != 2) {
            return
        }
        let bytes = UnsafePointer<UInt8>(msg.data!.bytes)
        let msgId: NSNumber = NSNumber(unsignedInt: 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1]))
        if (msgId.unsignedIntValue == 0) {
            return
        }
        var flow: MQttTxFlow? = txFlows.objectForKey(msgId) as? MQttTxFlow
        if (flow == nil) {
            return
        }
        var msg_copy = flow?.msg
        if (msg_copy?.type != MQTTMessageType.MQTTPublish.rawValue || msg_copy?.qos != 2) {
            return
        }
        msg_copy = MQTTMessage.pubrelMessageWithMessageId(msgId.unsignedShortValue)
        flow?.msg = msg
        self.timerRing?.objectAtIndex(Int(flow!.deadline) % 60).removeObject(msgId)
        flow?.deadline = ticks + 60
        self.timerRing?.objectAtIndex(Int(flow!.deadline) % 60).addObject(msgId)
        self.send(msg_copy!)
    }

    func handlePubrel (msg: MQTTMessage) {
        if (msg.data?.length != 2) {
            return
        }
        let bytes = UnsafePointer<UInt8>(msg.data!.bytes)
        let msgId: NSNumber = NSNumber(unsignedInt: 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1]))
        if (msgId.unsignedIntValue == 0) {
            return
        }
        var dict: NSDictionary? = rxFlows.objectForKey(msgId) as? NSDictionary
        if (dict != nil) {
            delegate?.session(self, newMessage: dict?.valueForKey("data") as NSData, onTopic: dict?.valueForKey("topic") as String)
            if let closure = messageHandler {
                closure(message: dict?.valueForKey("data") as NSData, topic: dict?.valueForKey("topic") as String)
            }
            txFlows.removeObjectForKey(msgId)
        }
        self.send(MQTTMessage.pubcompMessageWithMessageId(msgId.unsignedShortValue))
    }
    

    func handlePubcomp (msg: MQTTMessage) {
        if (msg.data?.length != 2) {
            return
        }        
        let bytes = UnsafePointer<UInt8>(msg.data!.bytes)
        let msgId: NSNumber = NSNumber(unsignedInt: 256 * CUnsignedInt(bytes[0]) + CUnsignedInt(bytes[1]))
        if (msgId.unsignedIntValue == 0) {
            return
        }
        var flow: MQttTxFlow? = txFlows.objectForKey(msgId) as? MQttTxFlow
        if (flow == nil || flow?.msg.type != MQTTMessageType.MQTTPubrel.rawValue) {
            return
        }
        self.timerRing?.objectAtIndex(Int(flow!.deadline) % 60).removeObject(msgId)
        txFlows.removeObjectForKey(msgId)
    }
    
    func error(eventCode: MQTTSessionEvent) {
        encoder?.close()
        encoder = nil
        decoder?.close()
        decoder = nil
        if (timer != nil) {
            timer?.invalidate()
            timer = nil
        }
        status = MQTTSessionStatus.Error
        usleep(1000000) // 1 sec delay
        delegate?.session(self, handleEvent: eventCode)
        
        if let closure = connectionHadler {
            closure(event: eventCode)
        }
    }
    
    func send (msg: MQTTMessage) {
        if (encoder?.status == MQTTEncoderStatus.Ready) {
            encoder?.encodeMessage(msg)
        } else {
            self.queue?.addObject(msg)
        }
    }
    
    func nextMsgId () -> UInt16 {
        txMsgId++
        while (txMsgId == 0 || txFlows.objectForKey(NSNumber(unsignedInt: UInt32(txMsgId))) != nil) {
            txMsgId++
        }
        return txMsgId
    }
}