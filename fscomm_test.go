/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

package comm

import ("testing"
	"log"
	"net"
	"time"
	"fmt")

import err "github.com/gvallee/syserror"

const namespace1 = "namespace_test1"
const namespace2 = "namespace2"
const block1 = 1
const offset1 = 128
const block2 = 42
const offset2 = 24

func sendFiniMsg (conn net.Conn) {
	myerr := SendMsg (conn, TERMMSG, nil)
	if (myerr != err.NoErr) { log.Fatal ("ERROR: SendMsg() failed") }

	time.Sleep (1 * time.Second) // Give a chance for the msg to arrive before we exit which could close the connection before the test completes
}

func TestServerCreation (t *testing.T) {
	fmt.Print ("Testing creation of a valid server ")

	// Create a server asynchronously
	server_info := CreateServerInfo ("127.0.0.1:8888", uint64(1024))
	go CreateEmbeddedServer (server_info)

	// Create a simple client that will just terminate everything
	conn, _ := Connect2Server ("127.0.0.1:8888")
	sendFiniMsg (conn)

	// Terminate global state
	fmt.Println ("PASS")
}

func runServer (info *ServerInfo) {
	fmt.Println ("Actually creating the server...")
	mysyserr := CreateServer (info)
	if (mysyserr != err.NoErr) { log.Fatal ("ERROR: Cannot create new server") }

	done := 0
	fmt.Println ("Waiting for connection handshake...")
	HandleHandshake (info.conn)
	for done != 1 {
		fmt.Println ("Receiving data...")
		namespace, blockid, offset, data, myerr := RecvData (info.conn)
		if (myerr != err.NoErr) { log.Fatal ("ERROR: Cannot recv data", myerr.Error()) }
		fmt.Println ("Receiving data for namespace", namespace, "for blockid", blockid, "at offset", offset, " - ", len (data), "bytes")
		if (namespace != namespace1) { log.Fatal ("ERROR: namespace mismatch") }
		if (blockid != block1) { log.Fatal ("ERROR: blockid mismatch") }
		if (offset != offset1) { log.Fatal ("ERROR: offset mismatch") }

		myerr = SendData (info.conn, namespace2, block2, offset2, []byte("OKAY"))
		if (myerr != err.NoErr) { log.Fatal ("ERROR: Cannot send data", myerr.Error()) }

		/* Wait for the termination message */
		fmt.Println ("Waiting for the termination message...")
		msgtype, _, _, _ := RecvMsg (info.conn)
		if (msgtype != TERMMSG) { log.Fatal ("ERROR: Received wrong type of msg") }

		done = 1
	}
	FiniServer ()
}

func TestSendRecv (t *testing.T) {
	fmt.Print ("Testing data send/recv ")
	url := "127.0.0.1:9889"

	// Create a server asynchronously
	fmt.Println ("Creating server...")
	server_info := CreateServerInfo (url, uint64(1024))
	go runServer (server_info)

	conn, myerr := Connect2Server (url)
	if (myerr != err.NoErr) { log.Fatal ("Client error: Cannot connect to server") }
	fmt.Println ("Successfully connected to server")

	fmt.Println ("Sending test data...")
	buff := make ([]byte, 512)
	fmt.Println ("Sending data...")
	syserr := SendData (conn, namespace1, block1, offset1, buff)
	if (syserr != err.NoErr) { log.Fatal ("ERROR: Impossible to send data") }

	fmt.Println ("Receiving ack...")
	namespace, blockid, offset, data, rerr := RecvData (conn)
	if (rerr != err.NoErr) { log.Fatal ("Client error: Cannot receive data", rerr.Error()) }
	if (namespace != namespace2) { log.Fatal ("Client error: Namespace mismatch") }
	if (blockid != block2) { log.Fatal ("Client error: blockid mismatch") }
	if (offset != offset2) { log.Fatal ("Client error: offset mismatch") }
	if (string(data) != "OKAY") { log.Fatal ("Client error: data mismatch") }

	fmt.Println ("Test completed, sending termination msg...")
	sendFiniMsg (conn)

	fmt.Println ("PASS")
}
