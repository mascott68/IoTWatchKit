//
//  MQTTClient.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 11/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

@objc
protocol MQTTConnectionStatusListener {
    func MQTTSessionConnected()
    func MQTTConnectionRefused()
    func MQTTConnectionClosed()
    func MQTTConnectionError()
}

@objc
protocol MQTTMessageListener {
    func MQTTMessageReceived (message: String, topic: String)
}

class MQTTClient: NSObject, MQTTSessionDelegate {
    var sessionStatusListeners: Array<MQTTConnectionStatusListener>
    var messageListeners: Array<MQTTMessageListener>
    var sharedMQTTClient: AnyObject?
    var currentHost: String = String()
    var currentPort: CInt = CInt()
    var currentClientID: String = String()
    var mqttsession: MQTTSession?
    
    class var sharedInstance : MQTTClient {
        struct Static {
            static var onceToken : dispatch_once_t = 0
            static var instance : MQTTClient? = nil
        }
        dispatch_once(&Static.onceToken) {
            Static.instance = MQTTClient()
        }
        return Static.instance!
    }
    
    override init () {
        sessionStatusListeners = Array<MQTTConnectionStatusListener>()
        messageListeners = Array<MQTTMessageListener>()
    }
    
//MARK: Connection Status Listeners Notifications
    
    func notifyConnected() {
        for listener in sessionStatusListeners {
            listener.MQTTSessionConnected()
        }
    }
    
    func notifyRefused() {
        for listener in sessionStatusListeners {
            listener.MQTTConnectionRefused()
        }
    }
    
    func notifyClosed() {
        for listener in sessionStatusListeners {
            listener.MQTTConnectionClosed()
        }
    }
    
    func notifyError() {
        for listener in sessionStatusListeners {
            listener.MQTTConnectionError()
        }
    }
    
//MARK: New Message Notifications
    
    func newMessageReceived(message:String, topic: String) {
        for listener in messageListeners {
            listener.MQTTMessageReceived(message, topic: topic)
        }
    }
    
//MARK: Send message
    
    func sendMessage (topic: String, msg: String) {
        let data = (msg as NSString).dataUsingEncoding(NSUTF8StringEncoding)
        mqttsession?.publishData(data!, topic: topic)
    }

//MARK: MQTT Callback methods

    func session(sender: MQTTSession, handleEvent: MQTTSessionEvent) {
        switch (handleEvent) {
        case MQTTSessionEvent.Connected:
            //println("connected")
            self.notifyConnected()
            break;
        case MQTTSessionEvent.ConnectionRefused:
            //println("connection refused")
            self.notifyRefused()
            break
        case MQTTSessionEvent.ConnectionClosed:
            //println("connection closed")
            self.notifyClosed()
            break
        case MQTTSessionEvent.ConnectionError:
            //println("connection error")
            self.notifyError()
            break
        case MQTTSessionEvent.ProtocolError:
            println("protocol error")
            break
        }
    }
    
    func session(sender: MQTTSession, newMessage: NSData, onTopic: String) {
        let payloadString: String = NSString(data: newMessage, encoding: NSUTF8StringEncoding)!
        self.newMessageReceived(payloadString, topic: onTopic)

    }

//MARK: Initializing MQTT Communications
    
    func setupMQTT(host: String, port: CInt, clientID: String) {
        self.currentHost = host
        self.currentClientID = clientID
        self.currentPort = port
        
        self.mqttsession = MQTTSession(aClientId: clientID)
        self.mqttsession?.connectToHost(host, port: UInt32(port))
        self.mqttsession?.delegate = self
    }
    
    func setupMQTT(host: String, port: CInt, clientID: String, username: String, password: String) {
        self.currentHost = host
        self.currentClientID = clientID
        
        self.mqttsession = MQTTSession(aClientId: clientID, aUserName: username, aPassword: password)
        self.mqttsession?.connectToHost(host, port: UInt32(port))
        self.mqttsession?.delegate = self
    }

    func setupMQTT(host: String, port: CInt, clientID: String, username: String, password: String, keepAlive: CInt, clean: Bool) {
        self.currentHost = host
        self.currentClientID = clientID
        
        self.mqttsession = MQTTSession(aClientId: clientID, aUserName: username, aPassword: password, aKeepAliveInterval: UInt16(keepAlive), aCleanSessionFlag: clean)
        self.mqttsession?.connectToHost(host, port: UInt32(port))
        self.mqttsession?.delegate = self
    }
    
    func setupMQTT(host: String, port: CInt, clientID: String, username: String, password: String, keepAlive: CInt, clean: Bool, topic: String, message: NSData, qosLevel: CInt, retainFlag: Bool) {
        self.currentHost = host
        self.currentClientID = clientID
        
        self.mqttsession = MQTTSession(aClientId: clientID, aUserName: username, aPassword: password, aKeepAliveInterval: UInt16(keepAlive), aCleanSessionFlag: clean, willTopic: topic, willMsg: message, willQoS: UInt8(qosLevel), willRetainFlag: retainFlag)
        self.mqttsession?.connectToHost(host, port: UInt32(port))
        self.mqttsession?.delegate = self
    }

    func closeMQTT () {
        messageListeners.removeAll()
        sessionStatusListeners.removeAll()
        self.mqttsession?.close()
        self.mqttsession = nil
    }
    
    func subscribeTopic (topic: String) {
        println("Subscribing on topic \(topic)")
        self.mqttsession?.subscribeTopic(topic)
    }

    func subscribeToTopic (topic: String, qosLevel: UInt8) {
        println("Subscribing on topic \(topic)")
        self.mqttsession?.subscribeToTopic(topic, qosLevel: qosLevel)
    }

    func unsubscribeTopic (topic: String) {
        println("Unsubscribing on topic \(topic)")
        self.mqttsession?.unsubscribeTopic(topic)
    }
    
//MARK: Different types of listeners
    
    func addMQTTConnectionStatusListener (listener: protocol<MQTTConnectionStatusListener>) {
        sessionStatusListeners.append(listener)
    }
    
    func removeMQTTConnectionStatusListener (listener: protocol<MQTTConnectionStatusListener>) {
        var sessionStatusListenersNEW: Array<MQTTConnectionStatusListener> = Array<MQTTConnectionStatusListener>()
        for elem in sessionStatusListeners {
            if elem !== listener {
                sessionStatusListenersNEW.append(elem)
            }
        }
        sessionStatusListeners.removeAll()
        sessionStatusListeners = sessionStatusListenersNEW
    }
  
//MARK: Add Remove Message Listeners
    
    func addMQTTMessageListener (listener: protocol<MQTTMessageListener>) {
        messageListeners.append(listener)
    }
    
    func removeMQTTMessageListener (listener: protocol<MQTTMessageListener>) {
        var messageListenersNEW: Array<MQTTMessageListener> = Array<MQTTMessageListener>()
        for elem in messageListeners {
            if elem !== listener {
                messageListenersNEW.append(elem)
            }
        }
        messageListeners.removeAll()
        messageListeners = messageListenersNEW
    }
}