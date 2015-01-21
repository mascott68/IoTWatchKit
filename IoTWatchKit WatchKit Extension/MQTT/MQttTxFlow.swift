//
//  MQttTxFlow.swift
//  MQTT_SWIFT
//
//  Created by Julia Matveeva on 09/12/14.
//  Copyright (c) 2014 2lemetry. All rights reserved.
//

import Foundation

class MQttTxFlow: NSObject {
    
    var msg: MQTTMessage
    var deadline: CUnsignedInt = CUnsignedInt()
    
    class func flowWithMsg(msg: MQTTMessage, deadline: CUnsignedInt) -> MQttTxFlow {
        return MQttTxFlow(aMsg: msg, aDeadline: deadline)
    }
    
    init(aMsg: MQTTMessage, aDeadline: CUnsignedInt) {
        msg = aMsg
        deadline = aDeadline
    }
}
