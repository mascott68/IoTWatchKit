//
//  MQTTDecoder.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 08/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

enum MQTTDecoderEvent {
    case ProtocolError
    case ConnectionClosed
    case ConnectionError
}

enum MQTTDecoderStatus {
    case Initializing
    case DecodingHeader
    case DecodingLength
    case DecodingData
    case ConnectionClosed
    case ConnectionError
    case ProtocolError
}

protocol MQTTDecoderDelegate {
    func decoder(sender: MQTTDecoder, msg: MQTTMessage)
    func decoder(sender: MQTTDecoder, handleEvent: MQTTDecoderEvent)
}

class MQTTDecoder: NSObject, NSStreamDelegate {
    
    var stream: NSInputStream?
    var runLoop: NSRunLoop = NSRunLoop()
    var runLoopMode: String = String()
    var header: UInt8 = UInt8()
    var length: UInt32 = UInt32()
    var lengthMultiplier: UInt32 = UInt32()
    var dataBuffer: NSMutableData?
    
    var delegate: MQTTDecoderDelegate?
    var status: MQTTDecoderStatus    
    
    init(aStream: NSInputStream, aRunLoop: NSRunLoop, aMode: String) {
        status = MQTTDecoderStatus.Initializing
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
        stream?.delegate = nil
        stream?.close()
        stream?.removeFromRunLoop(runLoop, forMode: runLoopMode)
        stream = nil
    }

    func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
        if stream == nil {
            return
        }
        switch eventCode {
        case NSStreamEvent.OpenCompleted:
            status = MQTTDecoderStatus.DecodingHeader
            break
        case NSStreamEvent.HasBytesAvailable:
            if status == MQTTDecoderStatus.DecodingHeader {
                var n: Int = stream!.read(&header, maxLength: 1)
                if (n == -1) {
                    status = MQTTDecoderStatus.ConnectionError
                    delegate?.decoder(self, handleEvent: MQTTDecoderEvent.ConnectionError)
                } else if (n == 1) {
                    length = 0
                    lengthMultiplier = 1
                    status = MQTTDecoderStatus.DecodingLength
                }
                while (status == MQTTDecoderStatus.DecodingLength) {
                    var digit: UInt8 = UInt8()
                    var n: Int = stream!.read(&digit, maxLength: 1)
                    if (n == -1) {
                        status = MQTTDecoderStatus.ConnectionError;
                        delegate?.decoder(self, handleEvent: MQTTDecoderEvent.ConnectionError)
                        break
                    } else if (n == 0) {
                        break
                    }
                    length += (UInt32(digit) & 0x7f) * lengthMultiplier
                    if ((digit & 0x80) == 0x00) {
                        dataBuffer = NSMutableData(capacity: Int(length))
                        status = MQTTDecoderStatus.DecodingData;
                    } else {
                        lengthMultiplier *= 128
                    }
                }
                if (status == MQTTDecoderStatus.DecodingData) {
                    if (length > 0) {
                        var n: Int
                        var toRead: Int
                        var buffer = Array<UInt8>(count: 768, repeatedValue: 0)
                        toRead = Int(length) - dataBuffer!.length
                        if toRead > buffer.count {
                            toRead = buffer.count
                        }
                        n = stream!.read(&buffer, maxLength: toRead)
                        if (n == -1) {
                            status = MQTTDecoderStatus.ConnectionError
                            delegate?.decoder(self, handleEvent: MQTTDecoderEvent.ConnectionError)
                        } else {
                            dataBuffer?.appendBytes(buffer, length: n)
                        }
                    }
                    if let dataBuffer_length = dataBuffer?.length {
                        if (dataBuffer_length == Int(length)) {
                            var type: UInt8
                            var qos: UInt8
                            var isDuplicate: Bool
                            var retainFlag: Bool

                            type = (header >> 4) & 0x0f
                            isDuplicate = false
                            if ((header & 0x08) == 0x08) {
                                isDuplicate = true
                            }
                            qos = (header >> 1) & 0x03
                            retainFlag = false
                            if ((header & 0x01) == 0x01) {
                                retainFlag = true
                            }
                            var msg: MQTTMessage =  MQTTMessage(aType: type, aQos: qos, aRetainFlag: retainFlag, aDupFlag: isDuplicate, aData: dataBuffer!)
                            delegate?.decoder(self, msg: msg)
                            dataBuffer = nil
                            status = MQTTDecoderStatus.DecodingHeader
                        }
                    }
                }
            }
            break
        case NSStreamEvent.EndEncountered:
            status = MQTTDecoderStatus.ConnectionClosed
            delegate?.decoder(self, handleEvent: MQTTDecoderEvent.ConnectionClosed)
            break            
        case NSStreamEvent.ErrorOccurred:
            status = MQTTDecoderStatus.ConnectionError
            delegate?.decoder(self, handleEvent: MQTTDecoderEvent.ConnectionError)
            break
        default:
            println("unhandled event code")
            break
        }
    }
}