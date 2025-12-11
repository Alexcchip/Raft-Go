# Raft-Go

**Just a few words on the project**

This project can be broken down into a few sections/parts. 

1. Imports/Struct definitions

For the imports this is where I import a few standard libraries such as time (for heartbeating), os to grab cmd line args, and encoding/json to (un)marshall my messages to send over the socket

Below that I have defined all of the structs/"types" that I will need such as a log Entry, a Replica, and a Command to represent a put. It's important to note here that Go doesn't use the concept of classes so I use structs and instances of them instead.

2. There are a variety of helper functions that are used quite often such as: sendMessage, sendToPeers (same as sendMessage but loops through all peers), and randomTimeoutGen (used for election timeouts)

3. The next most important part is the logic flow function and handleNewMessage function:

These two functions are entry points to the main sections. Using Go channels and select statements I was able to create a lifecycle loop that waits for timeouts and new messages. Messages are handled according to their type as well as the state of the replica

4. The last main part is the main function. Being the entrypoint into the program, this function grabs args, defines the socket, as well as the initial replica with its first attributes. After these definitions are created I call the logic flow, message listener, and the heartBeatSending in seperate go routines to prevent blocking and take advantage of Go's concurrency.

5. I would also like to talk about my canCommit() function. This function is used once information about last known commits of followers is known. The leader loops through from last applied to the last tracked commit and applies it to it's local state machine and sends an ack back to the original sender.


6. Another major addition is the use of mutexes. While small, this addition has allowed me to lower possible race conditions and lock shared resources.

7. Heartbeat channel. With the addition of heartbeat channels I am able to end heartbeating of any given replica on demotion by just closing the channel (prevents the illusion of several leaders live at once).
