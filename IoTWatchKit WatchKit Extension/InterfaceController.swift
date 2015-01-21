//
//  InterfaceController.swift
//  IoTWatchKit WatchKit Extension
//
//  Created by Marlin Scott on 1/21/15.
//  Copyright (c) 2015 2lemetry. All rights reserved.
//

import WatchKit
import Foundation


class InterfaceController: WKInterfaceController, MQTTConnectionStatusListener, MQTTMessageListener  {

    let mqttClient: MQTTClient = MQTTClient.sharedInstance as MQTTClient
    
    // Replace domain with the correct domain in your 2lemetry portal
    let topic: String = "DOMAIN/input/watch"
    
    @IBOutlet weak var message: WKInterfaceLabel!
    @IBOutlet weak var image: WKInterfaceImage!
    @IBOutlet weak var state: WKInterfaceLabel!
    @IBOutlet weak var `switch`: WKInterfaceSwitch!
    
   
    @IBAction func handleSwitch(value: Bool) {
        if value {
            mqttClient.sendMessage( topic, msg: "{\"test\":\"on\"}")
        } else {
            mqttClient.sendMessage( topic, msg: "{\"test\":\"off\"}")
        }
     }
    
    
    func setupMQTTClient () {
       // Replace username and password with correct 2lemetry credentials  https://app.thingfabric.com/#/auth
        
        mqttClient.setupMQTT("q.m2m.io", port: 1883, clientID: "watch", username: "1f93694f-9d41-48eb-a0ea-b82278fc870c", password: "548b4916c95c46fa0eb71d101b02efc8", keepAlive:30, clean:true);
//        mqttClient.setupMQTT("q.m2m.io", port: 1883, clientID: "watch", username: "USERNAME", password: "PASSWORD", keepAlive:30, clean:true);
        
    }
    
    // MQTT Listener Callback methods
    
    func MQTTSessionConnected () {
        println("MQTTSessionConnected")
        subscribeOnTopic();
    }
    
    func MQTTConnectionRefused () {
        println("MQTTConnectionRefused")
    }
    
    func MQTTConnectionClosed () {
        println("MQTTConnectionClosed")
    }
    
    func MQTTConnectionError () {
        println("MQTTConnectionError")
        subscribeOnTopic();
    }
    
    func MQTTMessageReceived(message: String, topic: String) {
        println("MQTTMessageReceived")
        println(message)
        
        self.message.setText(message);
        
        let data = message.dataUsingEncoding(NSUTF8StringEncoding)
        
        let json = JSON(data:data!);
        let value = json["test"];
        
        println("Test data: \(value)");
        state.setText(value.stringValue);
        image.setHidden(false);
        if value.stringValue == "on" {
            self.image.setImageNamed("on.png");
        } else if value.stringValue == "off"{
            self.image.setImageNamed("off.png");
        }
    }
    
    
    func subscribeOnTopic () {
        mqttClient.subscribeTopic(topic)
    }
    
    override func awakeWithContext(context: AnyObject?) {
        super.awakeWithContext(context)
        
        // Configure interface objects here.
        setupMQTTClient();
        mqttClient.addMQTTConnectionStatusListener(self)
        mqttClient.addMQTTMessageListener(self)
    }

    override func willActivate() {
        // This method is called when watch view controller is about to be visible to user
        super.willActivate()
    }

    override func didDeactivate() {
        // This method is called when watch view controller is no longer visible
        super.didDeactivate()
    }

}
