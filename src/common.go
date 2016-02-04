package paxos

const (
	OK = "OK"
	Reject = "Reject"
)

type PrepareArgs struct {
	Seq int
	PNum string
}

type PrepareReply struct {
	Err string
	AcceptPnum string
	AcceptValue interface {}
}

type AcceptArgs struct {
	Seq int
	PNum string
	Value interface {}
}

type AcceptReply struct  {
	Err string
}

type DecideArgs struct {
	Seq int
	Value interface {}
	PNum string
	Me int
	Done int
}

type DecideReply struct {

}