package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Command struct {
	key   string
	value string
}

type Entry struct {
	Index   int
	Term    int
	Cmd     Command
	SentSrc string //To keep track of who sent append
	SentMID string //To keep track of who sent append
}

type Log struct {
	Logs         []Entry
	LastLogIndex int
	LastLogTerm  int
}

type Replica struct {
	Log                  Log               //Log of entries
	Term                 int               //Current term
	SimulatorPort        int
	Store                map[string]string //K/V store
	Id                   string            //Replica ID
	Timer                time.Time         //Relative physical clock of replica for timeouts
	Peers                []string          //IDs of peers (from args)
	ElectionTimeout      time.Duration     //Duration of how often replica needs RPC before starting election
	State                ServerState       //follower/leader/candidate state
	LeaderId             string            //Current known leader ID
	Conn                 *net.UDPConn      //Replica's UDP conn for sending messages
	ListeningChannel     chan Message      //Channel that receives all messages (chan used for shared access)
	HeartbeatInterval    time.Duration     //How often to send heartbeatRPC
	Voting               bool              //In voting state
	SentVote             string            //Used to track last vote
	EndBeat 		     chan struct{}     //Used to stop heartbeat when no longer leader (prevens swarm)
	CommitIndex          int               //tracking last commit
	LastApplied          int               //index of last applied to k/v
	CurrentVoteState     VotingState       //Temporary state for when in voting state
	IndexToSend			 map[string]int    //Maps ID: next index to send
	ReplicatedKnown      map[string]int    //Tracks last KNOWN replicated idx
	SeenMIDs		     map[string]bool   //Tracks seen MIDs so doesn't double process
	mu 					 sync.Mutex		   //Mutex to remove race conditions and lock shared resources
	RespondedMID		 map[string]bool   //Tracks MIDs responded to (to prevent race conditions)
}

type VotingState struct {
	Votes     map[string]bool
	QuorumNum int
}

type ServerState int

const (
	Follower ServerState = iota //No construct of enum in go so int representation necessary
	Candidate
	Leader
)

type Message struct {
	Src          string  `json:"src"`
	Dst          string  `json:"dst"`
	MID          string  `json:"MID"`
	Leader       string  `json:"leader"`
	Type         string  `json:"type"`
	Key          *string `json:"key,omitempty"`
	Value        *string `json:"value,omitempty"`
	Term         int     `json:"term,omitempty"`
	LastLogIndex int     `json:"lastlogindex,omitempty"`
	LastLogTerm  int     `json:"lastlogterm,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	CommitIndex  int     `json:"commitindex,omitempty"`
	Voted        bool    `json:"voted,omitempty"`
	NeededIndex  int     `json:"neededindex,omitempty"`
	Committed    bool    `json:"committed,omitempty"`
	ConflictTerm int    `json:"conflictterm,omitempty"`
	ConflictIndex int    `json:"conflictindex,omitempty"`
	Success       bool   `json:"success,omitempty"`


}


func randomTimeoutGen() time.Duration {
	return time.Duration(rand.Int63n(400)+900) * time.Millisecond
}

//this is the main logic loop that runs in its own goroutine (deals with timeouts and new messages of any kind)
func (r *Replica) logicFlow() {
		for {
			r.mu.Lock()
			timeout := r.ElectionTimeout - time.Since(r.Timer)
			r.mu.Unlock()
			 if timeout < 0 {
				timeout = 0
			}
			//Creates a channel that returns every timeout (a ticker)
			timeoutChannel := time.After(timeout)

			select {
				case <-timeoutChannel:
					//Starts election
					r.handleTimeout()
				case newMsg := <-r.ListeningChannel:
					//Handles all messages received
					r.handleNewMessage(newMsg)
			}
	}
}

//both functions not implemented in standard go
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//Used for both heartbeating as well as sending slices of logs to be replicated
func (r *Replica) appendEntriesSend() {

	for _, id := range r.Peers {

		needed := max(1, r.IndexToSend[id])
		
		//defines the empty (heartbeat) log or slice of entries to be replicated
		var hbEntries []Entry
		if needed > r.Log.LastLogIndex {
			hbEntries = []Entry{} 
		} else {
			hbEntries = r.Log.Logs[needed-1:]
		}

		//gets the term of the entry before the ones  being sent
		var prevTerm int
		if  needed > 1 && len(r.Log.Logs) > 0 {
			prevTerm = r.Log.Logs[needed-2].Term
		}

		heartbeat := Message{
            Src:             r.Id,
            Dst:             id,
            MID:             genRandomStr(5),
            Leader:          r.Id,
            Type:            "AppendEntries",
            Term:            r.Term,
            LastLogIndex:    needed-1, // The index the Follower needs to match
            LastLogTerm:    prevTerm,  // The term the Follower needs to match
            CommitIndex:     r.CommitIndex, // Leader's commit index
            Entries:         hbEntries, // Empty for heartbeat, populated for replication
        }
		r.sendMessage(heartbeat)
	}	
}

//Running in a goroutine to send periodic heartbeats (AppendEntriesRPCs with empty Entries)
func (r *Replica) heartBeatSending(running <-chan struct{}) {
    // Use the HeartbeatInterval defined in the Replica struct (e.g., 200ms)
    ticker := time.NewTicker(r.HeartbeatInterval) 
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
			r.mu.Lock()
            if r.State == Leader { 
				//sends heartbeat OR entries to be replicated
                r.appendEntriesSend()
				//checks if any new logs can be committed, called to prevent race conditions and to stay uptodate
				r.canCommit()
            }
			r.mu.Unlock()
        case <-running:
			fmt.Printf("Heartbeat stopped")
            return
        }
    }
}

//a helper function that closes the heartbeat channel of a leader (only called when leader)
func (r *Replica) stopHeartBeat() {
	if r.EndBeat != nil {
		close(r.EndBeat)
		r.EndBeat = nil
	}
}

//handles all new messages and routes to logic given type
func (r *Replica) handleNewMessage(msg Message) {
	//locks resources to prevent race conditions
	r.mu.Lock()
	defer r.mu.Unlock()
	//candidate/follower does not care about get/put requests and redirects
	if (msg.Type == "get" || msg.Type == "put") && r.State != Leader {
		redirectMsg := Message{Src: r.Id, Dst: msg.Src, Leader: r.LeaderId, MID: msg.MID}
		if r.LeaderId == "FFFF" {
			redirectMsg.Type = "fail"
		} else {
			redirectMsg.Type = "redirect"
		}
		r.sendMessage(redirectMsg)
		r.RespondedMID[msg.MID] = true
		return
	}

	//preventing duplicate responses for unecessary load/race conditions
	if msg.Type == "get" || msg.Type == "put" {
		if r.RespondedMID[msg.MID] {
			return 
		}
	}

	if msg.Type == "put" {
		//checks to see if we've already added to log
		if r.SeenMIDs[msg.MID] {
			return
		}

		entry := Entry{Index: r.Log.LastLogIndex+1, Term: r.Term, Cmd: Command{key: *msg.Key, value: *msg.Value}, SentSrc: msg.Src, SentMID: msg.MID}
		r.Log.Logs = append(r.Log.Logs, entry)
		 r.Log.LastLogIndex++
		r.Log.LastLogTerm = r.Term
		r.SeenMIDs[msg.MID] = true
		//replicates to followers
		r.appendEntriesSend()
		return
	}

	if msg.Type == "get" {
		if r.RespondedMID[msg.MID] {
			return //don't want to respond to a request more than once
		}
		getResponse := Message{Src: r.Id, Type: "ok", Dst: msg.Src, Leader: r.LeaderId, MID: msg.MID}
		
		//checks to see if value is in K/V store
		if value, ok := r.Store[*msg.Key]; ok {
				getResponse.Value = &value
			} else {
				empty := ""
				getResponse.Value = &empty
			}
		r.sendMessage(getResponse)
		r.RespondedMID[msg.MID] = true
		return
	}

	if msg.Type == "AppendEntries" {
		switch r.State {
			case Follower:
				r.followerHandleAE(msg)
			case Candidate:
				if msg.Term >= r.Term {
					r.Timer = time.Now()
					r.State = Follower
					r.Voting = false
					r.SentVote = ""
					r.followerHandleAE(msg)
				}
			case Leader:
				if msg.Term > r.Term {
					r.Timer = time.Now()
					r.State = Follower
					r.Voting = false
					r.stopHeartBeat()
					r.SentVote = ""
					r.followerHandleAE(msg)
				} else {
					//ignore msg
				}
		}
		return
	}

	if msg.Type == "RequestVoteRPC" {
		response := Message{Src: r.Id, Term: r.Term, Dst: msg.Src, Leader: r.LeaderId, Type: "RequestVoteRPCBack"}
		if msg.Term > r.Term {
			r.LeaderId = "FFFF"
			r.Term = msg.Term
			r.State = Follower
			r.stopHeartBeat()
			r.SentVote = ""
			response.Term = r.Term
		} else if msg.Term < r.Term {
			response.Voted = false
			r.sendMessage(response)
			return
		} 

		if r.SentVote != "" && r.SentVote != msg.Src {
			response.Voted = false
			r.sendMessage(response)
			return
		}

		if msg.LastLogTerm > r.Log.LastLogTerm || 
		(msg.LastLogTerm == r.Log.LastLogTerm && msg.LastLogIndex >= r.Log.LastLogIndex) {
			response.Voted = true
			r.Timer = time.Now()
			r.SentVote = msg.Src
			r.ElectionTimeout = randomTimeoutGen()
		} else {
			response.Voted = false
		}
		r.sendMessage(response)
		return
	}


	if msg.Type == "RequestVoteRPCBack" {
		if r.State == Candidate {
			if msg.Term > r.Term {
				r.SentVote = ""
				r.Term = msg.Term
				r.State = Follower
				return
			} else if msg.Term == r.Term {
				r.CurrentVoteState.Votes[msg.Src] = msg.Voted
				//we count our replica as a vote
				tallies := 1
				for _, peerID := range r.Peers {
					if r.CurrentVoteState.Votes[peerID] {
						tallies++
					}
				}
				//compares votes to quorumnum
				if tallies >= r.CurrentVoteState.QuorumNum {
					r.State = Leader
					r.LeaderId = r.Id
					for _, id := range r.Peers {
						r.IndexToSend[id] = r.Log.LastLogIndex + 1 //empty/next index in most up to date log (new leader)
						r.ReplicatedKnown[id] = 0
					}
					r.EndBeat = make(chan struct{})
					go r.heartBeatSending(r.EndBeat)
				} 

			} else {
				fmt.Print("RequestVoteRPCBack fail term check")
				return
			}
		}
		return
	} 

	if msg.Type == "AppendEntriesRPCBack" {
		if r.State != Leader {
			return //only leaders care about these messages
		}

		if msg.Term > r.Term {
			//steps down to follower if response is ahead in term
			r.State = Follower
			r.Term = msg.Term
			r.stopHeartBeat()
			return
		}
		//advances known index to send for sender if can can (pre) commit
		if msg.Success {
			r.IndexToSend[msg.Src] = msg.NeededIndex+1
			r.ReplicatedKnown[msg.Src] = msg.NeededIndex
			//tries for consistency again
			r.canCommit()
		} else {
			r.IndexToSend[msg.Src] = msg.ConflictIndex
		}
		return
	}
}

func (r *Replica) canCommit() {
	for commitIdx := r.CommitIndex+1; commitIdx <= r.Log.LastLogIndex; commitIdx++{
		if r.Log.Logs[commitIdx-1].Term != r.Term  {
			continue
		}
		preCommitNum := 1
		for _, id := range r.Peers {
			if r.ReplicatedKnown[id] >= commitIdx {
				preCommitNum++
			}
		}
		 if preCommitNum > (len(r.Peers)+1)/2 {
            r.CommitIndex = commitIdx
        }
	}

	//apply ready commits to state machine
	for r.LastApplied < r.CommitIndex {
		r.LastApplied++
		toCommit := r.Log.Logs[r.LastApplied-1]
		r.Store[toCommit.Cmd.key] = toCommit.Cmd.value
		if !r.RespondedMID[toCommit.SentMID] {
			okResponse := Message{Src: r.Id, Dst: toCommit.SentSrc, Leader: r.LeaderId, Type: "ok", MID: toCommit.SentMID }
			r.RespondedMID[toCommit.SentMID] = true
			r.sendMessage(okResponse)
		}
	}
}

func (r *Replica) followerHandleAE(msg Message) {
	response := Message{Src: r.Id, Dst: msg.Src, Leader: r.LeaderId, MID: genRandomStr(5), Type: "AppendEntriesRPCBack", Term: r.Term, Success: false}
	//Accepts new message as leader when msg term > replica's current term
	if msg.Term >= r.Term {
		r.Timer = time.Now()
		r.LeaderId = msg.Leader
		r.Term = msg.Term
		response.Term = msg.Term
	} else {
		//Replica rejects the vote when the term is less than its own
		r.Timer = time.Now()
		r.sendMessage(response)
		return
	}


	if msg.LastLogIndex > r.Log.LastLogIndex {
		response.ConflictIndex = r.Log.LastLogIndex + 1
		r.sendMessage(response)
		return
	}
	var conflictingTerm int
	
	conflictingIndex := msg.LastLogIndex
	if msg.LastLogIndex > 0 && r.Log.Logs[msg.LastLogIndex-1].Term != msg.LastLogTerm {
		conflictingTerm = r.Log.Logs[msg.LastLogIndex-1].Term
		idx:= msg.LastLogIndex
		for idx > 0 && r.Log.Logs[idx-1].Term == conflictingTerm {
    		idx--
		}	
		conflictingIndex = idx +1
		response.ConflictTerm = conflictingTerm
		response.ConflictIndex = conflictingIndex
		r.sendMessage(response)
		return
	}

	if len(msg.Entries) >= 1 {
		if msg.LastLogIndex < len(r.Log.Logs) && r.Log.Logs[msg.LastLogIndex].Term != msg.Entries[0].Term {
			r.Log.Logs = r.Log.Logs[:msg.LastLogIndex]
		}
		r.Log.Logs = append(r.Log.Logs, msg.Entries...)
		r.Log.LastLogIndex = len(r.Log.Logs)
		r.Log.LastLogTerm = r.Log.Logs[r.Log.LastLogIndex - 1].Term
	}



	if msg.CommitIndex > r.CommitIndex {
        r.CommitIndex = min(msg.CommitIndex, r.Log.LastLogIndex)
		for r.LastApplied < r.CommitIndex {
			r.LastApplied++
			toApply := r.Log.Logs[r.LastApplied-1]
			r.Store[toApply.Cmd.key] = toApply.Cmd.value 
		}
	}
	response.NeededIndex = msg.LastLogIndex + len(msg.Entries)
	response.Success = true
	r.sendMessage(response)
}

func (r *Replica) handleTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	//On timeout the leader doesn't care or do anything
	if r.State == Follower || r.State == Candidate {
		r.Term++
		r.State = Candidate
		r.SentVote = r.Id
		r.Timer = time.Now()
		r.LeaderId = "FFFF"
		r.stopHeartBeat()
		requestVote := Message{Src: r.Id, Leader: r.LeaderId,Type: "RequestVoteRPC", Term: r.Term, 
		LastLogIndex: r.Log.LastLogIndex, LastLogTerm: r.Log.LastLogTerm, MID: genRandomStr(5),}
		r.CurrentVoteState = VotingState{Votes: make(map[string]bool), QuorumNum: ((len(r.Peers)+1)/2)+1}
		r.CurrentVoteState.Votes[r.Id] = true
		//Starts the election by sending this a RequestVoteRPC to everyone
		for _, id := range r.Peers {
			requestVote.Dst = id
			r.sendMessage(requestVote)
		}
	}
}

func (r *Replica) messageListener() {
	b := make([]byte, 65536)
	for {
		n, err := r.Conn.Read(b)
		if err != nil {
			fmt.Printf("Error reading heartbeat")
		}
		msgAllocated := make([]byte, n)
		copy(msgAllocated, b[:n])
		
		//Unmarshalls and unpacks into Message struct
		var unpackedMsg Message
		if err := json.Unmarshal(msgAllocated, &unpackedMsg); err != nil {
			fmt.Printf("Couldn't unpack heartbeat")
		}
		//Sends to message channel and detected on select{}
		r.ListeningChannel <- unpackedMsg
	}
}

func (r *Replica) sendMessage(msg Message) {
	if msg.Dst == r.Id {
		r.ListeningChannel <- msg
		return
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	destAddr := &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: r.SimulatorPort,
	}
	
	if r.Conn != nil {
		r.Conn.WriteToUDP(data, destAddr)
	}
}
// Generates random string for MIDs
func genRandomStr(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	buffer := make([]byte, length)
	for i := range buffer {
		buffer[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(buffer)
}

func main() {
	args := os.Args[1:]
	simulatorPort, _ := strconv.Atoi(args[0])
	id := args[1]
	peers := args[2:]

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(fmt.Errorf("error listening on UDP: %w", err))
	}
	
	if err := conn.SetReadBuffer(65536); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not set read buffer\n")
	}
    defer conn.Close()
	replica := Replica{
		Store: make(map[string]string),
		Id: id,
		Conn: conn,
		Timer: time.Now(),
		Peers: peers,
		SimulatorPort: simulatorPort,
		RespondedMID: map[string]bool{},
		ElectionTimeout: randomTimeoutGen(),
		HeartbeatInterval: 100 * time.Millisecond,
		SeenMIDs: make(map[string]bool),
		IndexToSend: make(map[string]int),
		ReplicatedKnown: make(map[string]int),
		Voting: false,
		Log: Log{
			LastLogIndex: 0,
			LastLogTerm: 0,
			Logs: make([]Entry, 0),
		},
		CommitIndex: 0,
		LeaderId: "FFFF",
		State: Follower,
		ListeningChannel: make(chan Message, 100),

	}
	initMsg := Message{
		Src: id,
		Dst: "FFFF",
		Type: "hello",
		MID: genRandomStr(5),
		Leader: "FFFF",
	}
	replica.sendMessage(initMsg)
	go replica.logicFlow()
	go replica.messageListener()
	select{}
}