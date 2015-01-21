//
//  MQTTEncoder.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 08/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

enum MQTTEncoderEvent {
    case Ready
    case ErrorOccurred
}

enum MQTTEncoderStatus {
    case Initializing
    case Ready
    case Sending
    case EndEncountered
    case Error
}

protocol MQTTEncoderDelegate {
    func encoder(sender: MQTTEncoder, handleEvent: MQTTEncoderEvent)
}

class MQTTEncoder: NSObject, NSStreamDelegate {
    
    var stream: NSOutputStream?
    var runLoop: NSRunLoop = NSRunLoop()
    var runLoopMode: String = String()
    var buffer: NSMutableData?
    var byteIndex: Int = Int()

    var delegate: MQTTEncoderDelegate?
    var status: MQTTEncoderStatus

    init(aStream: NSOutputStream, aRunLoop: NSRunLoop, aMode: String) {
        status = MQTTEncoderStatus.Initializing
        stream = aStream
        runLoop = aRunLoop
        runLoopMode = aMode
    }
    
    func open () {
        stream?.delegate = self
        stream?.scheduleInRunLoop(runLoop, forMode: runLoopMode)
        stream?.open()        
    }
    
    func close () {
        stream?.close()
        stream?.delegate = nil
        stream?.removeFromRunLoop(runLoop, forMode: runLoopMode)
        stream = nil
    }

    func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
        if stream == nil {
            return
        }
        assert(aStream == stream)
        switch eventCode {
        case NSStreamEvent.OpenCompleted:
            break
        case NSStreamEvent.HasSpaceAvailable:
            if (status == MQTTEncoderStatus.Initializing) {
                status = MQTTEncoderStatus.Ready
                delegate?.encoder(self, handleEvent: MQTTEncoderEvent.Ready)
            } else if (status == MQTTEncoderStatus.Ready) {
                delegate?.encoder(self, handleEvent: MQTTEncoderEvent.Ready)
            } else if (status == MQTTEncoderStatus.Sending) {
                var ptr: UInt8
                var n: Int
                var length: Int
                
                // Number of bytes pending for transfer
                length = buffer!.length - byteIndex
                n = stream!.write(UnsafePointer<UInt8>(buffer!.bytes + byteIndex), maxLength: length)

                if (n == -1) {
                    status = MQTTEncoderStatus.Error
                    delegate?.encoder(self, handleEvent: MQTTEncoderEvent.ErrorOccurred)
                } else if (n < length) {
                    byteIndex += n
                } else {
                    buffer = nil
                    byteIndex = 0
                    status = MQTTEncoderStatus.Ready
                }
            }
            break
        case NSStreamEvent.ErrorOccurred:
            break
        case NSStreamEvent.EndEncountered:
            if (status != MQTTEncoderStatus.Error) {
                status = MQTTEncoderStatus.Error
                delegate?.encoder(self, handleEvent: MQTTEncoderEvent.ErrorOccurred)
            }
            break
        default:
            println("Oops, event code not handled: \(eventCode)")
            break
        }
    }

    func encodeMessage (msg: MQTTMessage) {
        
        var header: UInt8 = UInt8()
        var n: Int = Int()
        var length: Int = Int()
        
        if (status != MQTTEncoderStatus.Ready) {
            println("Encoder not ready")
            return
        }
        
        assert (buffer == nil)
        assert (byteIndex == 0)
        
        buffer = NSMutableData()
        
        // encode fixed header
        header = msg.type << 4
        if (msg.isDuplicate == true) {
            header |= 0x08
        }
        header |= msg.qos << 1
        if (msg.retainFlag == true) {
            header |= 0x01
        }
        buffer?.appendBytes(&header, length: 1)
        
        // encode remaining length
        
        if msg.data != nil {
            length = msg.data!.length
            do {
                var digit: UInt8 = UInt8(length) % 128
                length /= 128
                if (length > 0) {
                    digit |= 0x80
                }
                buffer?.appendBytes(&digit, length: 1)
            } while (length > 0)
        }

        // encode message data
        if (msg.data != nil) {
            buffer?.appendData(msg.data!)
        }
        n = stream!.write(UnsafePointer<UInt8>(buffer!.bytes), maxLength: buffer!.length)
        if (n == -1) {
            status = MQTTEncoderStatus.Error
            delegate?.encoder(self, handleEvent: MQTTEncoderEvent.ErrorOccurred)
        } else if (n < buffer?.length) {
            byteIndex += n
            status = MQTTEncoderStatus.Sending
        } else {
            buffer = nil
        }
    }
}