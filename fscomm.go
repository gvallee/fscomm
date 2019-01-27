/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

package comm

import ("fmt"
	"encoding/binary"
	"time"
	"net")

import err "github.com/gvallee/syserror"

// Messages types
var INVALID = "INVA" // Invalid msg
var TERMMSG = "TERM" // Termination message
var CONNREQ = "CONN" // Connection request (initiate a connection handshake
var CONNACK = "CACK" // Response to a connection request (Connection ack)
var DATAMSG = "DATA" // Data msg, associated to a write operation
var READREQ = "READ" // Data request, associated to a read operation
var RDREPLY = "RRPL" // Reply to a READREQ, including the data

// Structure to store server information (host we connect to)
type ServerInfo struct {
	conn		net.Conn
	url		string
	blocksize	uint64
	timeout		int	// Timeout for accept(), no timeout if set to zero
}

// State of the local server if it exists (we can be only one server at a time)
var LocalServer *ServerInfo = nil

/**
 * Return the connection from the server info structure. This allows us to have
 * external components access the connection structure and use it
 * @param[in]   info    Data server info's data structure
 * @return      Connection structure
 * @return      System error handle
 */
func GetConnFromInfo (info *ServerInfo) (net.Conn, err.SysError) {
	if (info == nil) { return nil, err.ErrNotAvailable }
	return info.conn, err.NoErr
}

/**
 * Receive and parse a message header (4 character)
 * @param[in]	conn	Active connection from which to receive data
 * @return	Message type (string)
 * @return	System error handler
 */
func GetHeader (conn net.Conn) (string, err.SysError) {
	if (conn == nil) { return INVALID, err.ErrNotAvailable }

	hdr := make ([]byte, 4)
	if (hdr == nil) { return INVALID, err.ErrOutOfRes }

	/* read the msg type */
	s, myerr := conn.Read (hdr)
	if (myerr != nil) { fmt.Println ("ERROR:", myerr.Error()); return INVALID, err.ErrFatal }

	// Connection is closed
	if (s == 0) {
		fmt.Println ("Connection closed")
		return TERMMSG, err.NoErr
	}

	// Read returns an error (we test this only as a second test since s=0 means socket closed, whic returns also EOF as an error but we want to handle it as normal termination, not an error case
	if (myerr != nil) { fmt.Println ("ERROR:", myerr.Error()); return INVALID, err.ErrFatal }

	if (s > 0 && s != 4) {
		fmt.Println ("ERROR Cannot recv header")
		return INVALID, err.NoErr
	}

	// Disconnect request
	if (string(hdr[:s]) == TERMMSG) {
		fmt.Println ("Recv'd disconnect request")
		return TERMMSG, err.NoErr
	}

	if (string(hdr[:s]) == CONNREQ) {
		fmt.Println ("Recv'd connection request")
		return CONNREQ, err.NoErr
	}

	if (string(hdr[:s]) == CONNACK) {
		fmt.Println ("Recv'd connection ACK")
		return CONNACK, err.NoErr
	}

	if (string(hdr[:s]) == DATAMSG) {
		fmt.Println ("Recv'd data message")
		return DATAMSG, err.NoErr
	}

	if (string(hdr[:s]) == READREQ) {
		fmt.Println ("Recv'd read request")
		return READREQ, err.NoErr
	}

	if (string(hdr[:s]) == RDREPLY) {
		fmt.Println ("Recv'd read reply")
		return RDREPLY, err.NoErr
	}

	fmt.Println ("Invalid msg header")
	return INVALID, err.ErrFatal
}

/**
 * Receive the payload size.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Payload size (can be zero)
 * @return	System error handle
 */
func getPayloadSize (conn net.Conn) (uint64, err.SysError) {
	ps := make ([]byte, 8) // Payload size is always 8 bytes
	s, myerr := conn.Read (ps)
	if (s != 8 || myerr != nil) {
		fmt.Println ("ERROR: expecting 8 bytes but received", s)
		return 0, err.ErrFatal
	}

	return binary.LittleEndian.Uint64(ps), err.NoErr
}

/**
 * Receive the payload.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Payload in []byte
 * @return	System error handle
 */
func getPayload (conn net.Conn, size uint64) ([]byte, err.SysError) {
	payload := make ([]byte, size)
	s, myerr := conn.Read (payload)
	if (uint64(s) != size || myerr != nil) {
		fmt.Println ("ERROR: expecting ", size, "but received", s)
		return nil, err.ErrFatal
	}

	return payload, err.NoErr
}

/**
 * Function called upon the reception of a read request. It is assumed that the message
 * header already has been received. This returns all the necessary data required by the
 * data server to read and return data from a given block.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Namespace's name of the read request
 * @return	Block id of the read request
 * @return	Offset of the read request
 * @return	Size of the read request
 * @return	System error handle
 */
func HandleReadReq (conn net.Conn) (string, uint64, uint64, uint64, err.SysError) {
	var namespace_len uint64
	var namespace string
	var blockid uint64
	var offset uint64
	var size uint64
	var myerr err.SysError

	// Recv the length of the namespace
        namespace_len, myerr = RecvUint64 (conn)
        if (myerr != err.NoErr) { return "", 0, 0, 0, myerr }

        // Recv the namespace
        namespace, myerr = RecvNamespace (conn, namespace_len)
        if (myerr != err.NoErr) { return "", 0, 0, 0, myerr }

        // Recv the blockid (8 bytes)
        blockid, myerr = RecvUint64 (conn)
        if (myerr != err.NoErr) { return "", 0, 0, 0, myerr }

        // Recv the offset
        offset, myerr = RecvUint64 (conn)
        if (myerr != err.NoErr) { return "", 0, 0, 0, myerr }

        // Recv data size
        size, myerr = RecvUint64 (conn)
        if (myerr != err.NoErr) { return "", 0, 0, 0, myerr }

        return namespace, blockid, offset, size, err.NoErr
}

/**
 * Handle a connection request message, i.e., send a CONNACK message with the blocksize
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Size of the payload included in the CONNREQ message
 * @param[in]	payload	Payload included in the CONNREQ message
 * @return	System error handle
 */
func handleConnReq (conn net.Conn, size uint64, payload []byte) err.SysError {
	if (LocalServer == nil) {
		fmt.Println ("ERROR: local server is not initialized")
		return err.ErrFatal
	}

	// Send CONNACK with the block size
	buff := make ([]byte, 8) // 8 bytes for the block size
	binary.LittleEndian.PutUint64 (buff, LocalServer.blocksize)
	syserr := SendMsg (conn, CONNACK, buff)
	if (syserr != err.NoErr) { return syserr }

	return err.NoErr
}

/**
 * Receive and handle a CONNREQ message, i.e., a client trying to connect
 * @param[in]   conn    Active connection from which to receive data
 * @return	System error handle
 */
func HandleHandshake (conn net.Conn) err.SysError {
	/* Handle the CONNREQ message */
	msgtype, payload_size, payload, syserr := RecvMsg (conn)
	if (syserr != err.NoErr) { return err.ErrFatal }

	if (msgtype == CONNREQ) {
		handleConnReq (conn, payload_size, payload)
	} else {
		return err.NoErr // We did not get the expected CONNREQ msg
	}

	return err.NoErr
}

/**
 * Create an embeded server loop, i.e., an internal server that only receives and
 * do a basic parsing of messages. Mainly for testing.
 * @param[in]   conn    Active connection from which to receive data
 * @return	System error handle
 */
func doServer (conn net.Conn) err.SysError {
	done := 0

	syserr := HandleHandshake (conn)
	if (syserr != err.NoErr) { fmt.Println ("ERROR: Error during handshake with client") }
	for (done != 1) {
		// Handle the message header
		msgtype, syserr := GetHeader (conn)
		if (syserr != err.NoErr) { done = 1 }
		if (msgtype == "INVA" || msgtype == "TERM") { done = 1 }

		//  Handle the paylaod size
		payload_size, pserr := getPayloadSize (conn)
		if (pserr != err.NoErr) { done = 1 }

		// Handle the payload when necessary
		var payload []byte = nil
		if (payload_size != 0) {
			payload, syserr = getPayload (conn, payload_size)
		}
		if (payload_size != 0 && payload != nil) { return err.ErrFatal }
	}

	conn.Close ()
	FiniServer ()

	return err.NoErr
}

/**
 * Finalize a server.
 */
func FiniServer () {
	LocalServer = nil
}

/**
 * Create an embedded server, mainly for testing since we cannot customize how messages are
 * handled.
 * @param[in]	info	Structure representing the information about the server to create.
 * @return	System error handle
 */
func CreateEmbeddedServer (info *ServerInfo) err.SysError {
	if (info == nil) { return err.ErrFatal }

	if (LocalServer != nil) {
		fmt.Println ("ERROR: Local server already instantiated")
		return err.ErrFatal
	}

	LocalServer = info
	listener, myerr := net.Listen ("tcp", info.url)
	if (myerr != nil) { return err.ErrFatal }

	fmt.Println ("Server created on", info.url)

	var conn net.Conn
	for {
		conn, myerr = listener.Accept()
		info.conn = conn
		if (myerr != nil) { fmt.Println ("ERROR: ", myerr.Error()) }

		go doServer (conn)
	}

	return err.NoErr
}

/**
 * Create a generic server. Note that the connection handle from clients need to be explicitely
 * added to the server's code.
 * @param[in]	info	Structure representing the information about the server to create.
 * @return	Systemn error handle
 */
func CreateServer (info *ServerInfo) err.SysError {
	if (info == nil) { return err.ErrFatal }

	if (LocalServer != nil) {
		fmt.Println ("ERROR: Local server already instantiated")
		return err.ErrFatal
	}

	LocalServer = info
	listener, myerr := net.Listen ("tcp", info.url)
	if (myerr != nil) { fmt.Println (myerr.Error()); return err.ErrFatal }

	if (info.timeout > 0) {
		listener.(*net.TCPListener).SetDeadline (time.Now ().Add (time.Duration (info.timeout) * time.Second))
	}

	info.conn, myerr = listener.Accept()

	return err.NoErr
}

/**
 * Connect to a server that is listening for incoming connection request
 * @param[in[	url	URL of the server to connect to
 * @return	Connection handle to the server
 * @return	Data server's block size
 * @return	System error handle
 */
func Connect2Server (url string) (net.Conn, uint64, err.SysError) {
	conn, neterr := net.Dial ("tcp", url)
	if (neterr != nil) {
		fmt.Println ("ERROR: ", neterr.Error())
		return nil, 0, err.ErrOutOfRes
	}

	bs, myerr := ConnectHandshake (conn)
	if (bs == 0 || myerr != err.NoErr) { return nil, 0, err.ErrFatal }

	return conn, bs, err.NoErr
}

/**
 * Send a 8 bytes integer. Extremely useful to exchange IDs.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	value	Value to send (uint64)
 * @return	System error handle
 */
func sendUint64 (conn net.Conn, value uint64) err.SysError {
	if (conn == nil) { return err.ErrFatal }

	buff := make ([]byte, 8)
	if (buff == nil) { return err.ErrOutOfRes }

	binary.LittleEndian.PutUint64 (buff, value)
	s, myerr := conn.Write (buff)
	if (myerr != nil) { fmt.Println (myerr.Error()) }
	if (myerr == nil && s != 8) { fmt.Println ("Received ", s, "bytes, instead of 8") }
	if (s != 8 || myerr != nil) { return err.ErrFatal }

	return err.NoErr
}

/**
 * Receive a 8 bytes integer. Extremely usefuol to exchange IDs.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Received 8 bytes integer
 * @return	System error handle
 */
func RecvUint64 (conn net.Conn) (uint64, err.SysError) {
	if (conn == nil) { return 0, err.ErrFatal }

	msg := make ([]byte, 8)
	if (msg == nil) { return 0, err.ErrOutOfRes }

	s, myerr := conn.Read (msg)
	if (s != 8 || myerr != nil || msg == nil) { return 0, err.ErrFatal }

	return binary.LittleEndian.Uint64(msg), err.NoErr
}

/**
 * Send the message type (4 bytes)
 * @param[in]   conn    Active connection from which to receive data
 * @parma[in]	msgType	Msg type to send
 * @return	System error handle
 */
func sendMsgType (conn net.Conn, msgType string) err.SysError {
	if (conn == nil) { return err.ErrFatal }

	s, myerr := conn.Write ([]byte(msgType))
	if (s == 0 || myerr != nil) { return err.ErrFatal }

	return err.NoErr
}

/**
 * Receive a message type (4 bytes)
 * @param[in]   conn    Active connection from which to receive data
 * @return	Message type (string)
 * @return	System error handle
 */
func recvMsgType (conn net.Conn) (string, err.SysError) {
	if (conn == nil) { return "", err.ErrFatal }

	msgtype, syserr := GetHeader (conn)
	if (syserr != err.NoErr) { return "", err.ErrFatal }

	return msgtype, err.NoErr
}

/**
 * Send data.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	data	buffer to send ([]byte)
 * @return	System error handle
 */
func sendData (conn net.Conn, data []byte) err.SysError {
	if (conn == nil) { return err.ErrFatal }

	s, myerr := conn.Write (data)
	if (s == 0 || myerr != nil) { return err.ErrFatal }

	return err.NoErr
}

/**
 * Receive data; the receive buffer is automatically allocated.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Amount of data to receive (in bytes)
 * @return	Buffer with the received data ([]byte)
 * @return	System error handle
 */
func DoRecvData (conn net.Conn, size uint64) ([]byte, err.SysError) {
	data := make ([]byte, size)
	if (data == nil) { return nil, err.ErrOutOfRes }

	s, myerr := conn.Read (data)
	if (myerr != nil || uint64(s) != size || data == nil) { return nil, err.ErrFatal }

	return data, err.NoErr
}

/**
 * Send a namespace (string)
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	namespace	Namespace's name to send
 * @return	System error handle
 */
func sendNamespace (conn net.Conn, namespace string) err.SysError {
	s, myerr := conn.Write ([]byte(namespace))
	if (s == 0 || myerr != nil) { return err.ErrFatal }

	return err.NoErr
}

/**
 * Receive a namespace (string)
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Length of the namespace's name to receive
 * @return	Namespace's name (string)
 * @return	System error handle
 */
func RecvNamespace (conn net.Conn, size uint64) (string, err.SysError) {
	if (conn == nil) { return "", err.ErrFatal }

	buff := make ([]byte, size)
	if (buff == nil) { return "", err.ErrFatal }

	s, myerr := conn.Read (buff)
	if (myerr != nil || uint64(s) != size || buff == nil) { return "", err.ErrFatal }

	return string(buff), err.NoErr
}

/**
 * Send a read request to a data server.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]   namespace       Namespace's name in the context of which the data is sent
 * @param[in]   blockid         Block id where the data needs to be saved
 * @param[in]   offset          Block offset where the data needs to be saved
 * @param[in]	size		Amount of data to read
 */
func SendReadReq (conn net.Conn, namespace string, blockid uint64, offset uint64, size uint64) err.SysError {
	// Send the msg type
        myerr := sendMsgType (conn, DATAMSG)
        if (myerr != err.NoErr) { return myerr }

        // Send the length of the namespace
        var nslen uint64 = uint64 (len (namespace))
        myerr = sendUint64 (conn, nslen)
        if (myerr != err.NoErr) { return myerr }

        // Send the namespace
        myerr = sendNamespace (conn, namespace)
        if (myerr != err.NoErr) { return myerr }

        // Send the blockid (8 bytes)
        myerr = sendUint64 (conn, blockid)
        if (myerr != err.NoErr) { return myerr }

        // Send the offset
        myerr = sendUint64 (conn, offset)
        if (myerr != err.NoErr) { return myerr }

        // Send data size
        myerr = sendUint64 (conn, size)
        if (myerr != err.NoErr) { return myerr }

	return err.NoErr
}

/**
 * Send a buffer
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	namespace	Namespace's name in the context of which the data is sent
 * @param[in] 	blockid		Block id where the data needs to be saved
 * @param[in]	offset		Block offset where the data needs to be saved
 * @param[in]	data		Data to send
 * @return	System error handle
 */
func SendData (conn net.Conn, namespace string, blockid uint64, offset uint64, data []byte) err.SysError {
	// Send the msg type
	myerr := sendMsgType (conn, DATAMSG)
	if (myerr != err.NoErr) { return myerr }

	// Send the length of the namespace
	var nslen uint64 = uint64 (len (namespace))
	myerr = sendUint64 (conn, nslen)
	if (myerr != err.NoErr) { return myerr }

	// Send the namespace
	myerr = sendNamespace (conn, namespace)
	if (myerr != err.NoErr) { return myerr }

	// Send the blockid (8 bytes)
	myerr = sendUint64 (conn, blockid)
	if (myerr != err.NoErr) { return myerr }

	// Send the offset
	myerr = sendUint64 (conn, offset)
	if (myerr != err.NoErr) { return myerr }

	// Send data size
	myerr = sendUint64 (conn, uint64 (len (data)))
	if (myerr != err.NoErr) { return myerr }

	// Send the actual data
	myerr = sendData (conn, data)
	if (myerr != err.NoErr) { return myerr }

	return err.NoErr
}

/**
 * Send a basic message
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	msgType	Type of the message to send
 * @param[in]	Payload to associte to the message
 * @return 	System error handle
 */
func SendMsg (conn net.Conn, msgType string, payload []byte) err.SysError {
	if (conn == nil) { return err.ErrFatal }

	hdrerr := sendMsgType (conn, msgType)
	if (hdrerr != err.NoErr) { return hdrerr }

	if (payload != nil) {
		sendUint64 (conn, uint64 (len (payload)))
		conn.Write (payload)
	} else {
		sendUint64 (conn, uint64 (0))
	}

	return err.NoErr
}

/**
 * Receive data to be stored
 * @param[in]   conn    Active connection from which to receive data
 * @return	Namespace's name for which the data is received
 * @return	Blockid where the data needs to be saved
 * @return	Block offset where the data needs to be saved
 * @return	Buffer with the received data
 * @return	System error handle
 */
func RecvData (conn net.Conn) (string, uint64, uint64, []byte, err.SysError) {
	// Recv the msg type
	msgtype, terr := recvMsgType (conn)
	if (terr != err.NoErr || msgtype != DATAMSG) { return "", 0, 0, nil, terr }

	// Recv the length of the namespace
	nslen, syserr := RecvUint64 (conn)
	if (syserr != err.NoErr) { return "", 0, 0, nil, syserr }

	// Recv the namespace
	namespace, nserr := RecvNamespace (conn, nslen)
	if (nserr != err.NoErr) { return "", 0, 0, nil, nserr }

	// Recv blockid
	blockid, berr := RecvUint64 (conn)
	if (berr != err.NoErr) { return namespace, 0, 0, nil, berr }

	// Recv offset
	offset, oerr := RecvUint64 (conn)
	if (oerr != err.NoErr) { return namespace, blockid, 0, nil, oerr }

	// Recv data size
	size, serr := RecvUint64 (conn)
	if (serr != err.NoErr) { return namespace, blockid, offset, nil, serr }

	// Recv the actual data
	data, derr := DoRecvData (conn, size)
	if (derr != err.NoErr) { return namespace, blockid, offset, nil, derr }

	return namespace, blockid, offset, data, err.NoErr
}

/**
 * Receive a generic message
 * @param[in]   conn    Active connection from which to receive data
 * @return	Message type
 * @return	Payload size included in the message
 * @return	Payload ([]byte)
 * @return	System error handle
 */
func RecvMsg (conn net.Conn) (string, uint64, []byte, err.SysError) {
	if (conn == nil) { return "", 0, nil, err.ErrFatal }

	msgtype, myerr := GetHeader (conn)
	// Messages without payload
	if (msgtype == "TERM" || msgtype == "INVA" || myerr != err.NoErr) { return msgtype, 0, nil, err.ErrFatal }

	// Get the payload size
	payload_size, pserr := getPayloadSize (conn)
	if (pserr != err.NoErr) { return msgtype, 0, nil, err.ErrFatal }
	if (payload_size == 0) { return msgtype, 0, nil, err.NoErr }

	// Get the payload
	buff, perr := getPayload (conn, payload_size)
	if (perr != err.NoErr) { return msgtype, payload_size, buff, err.NoErr }

	return msgtype, payload_size, buff, err.NoErr
}

/**
 * Initiate a connection handshake. Required for a client to successfully connect to a server.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Server's block size
 * @return	System error handle
 */
func ConnectHandshake (conn net.Conn) (uint64, err.SysError) {
	myerr := SendMsg (conn, CONNREQ, nil)
	if (myerr != err.NoErr) { return 0, myerr }

	// Receive the CONNACK, the payload is the block_sizw
	msgtype, s, buff, recverr := RecvMsg (conn)
	if (recverr != err.NoErr || msgtype != CONNACK || s != 8 || buff == nil) { return 0, err.ErrFatal }
	block_size := binary.LittleEndian.Uint64(buff)
	if (LocalServer != nil) { LocalServer.blocksize = block_size }

	return block_size, err.NoErr
}

/**
 * Create an info handle that stores all the server info.
 * @param[in]	url	URL of the targer server
 * @param[in]	blocksize	Block size of the server.
 * @return	Pointer to a server info structure.
 */
func CreateServerInfo (url string, blocksize uint64, timeout int) *ServerInfo {
	s := new (ServerInfo)
	if (s == nil) { return nil }

	s.blocksize = blocksize
	s.url = url
	s.timeout = timeout
	s.conn = nil

	return s
}

