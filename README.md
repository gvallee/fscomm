# Overview

This code is providing a simple communication layer for our user-level
file system. The goal of this layer is to:
- provide all the mechanisms to establish connections between client and data servers.
- when writing to the file system, transfer the data to be saved in blocks on target data servers
- when reading from the file system, send a read request and get the data back from the data servers.

# Messages

## Message Structure

This communication layer supports 2 types of message:
- basic message, which are composed on a message type, a payload size and a payload
- data message, which are composed on type (DATAMSG), a namespace length, a namespace, a block id, a block offset, a payload size, and finally the data.

### Basic Message Structure

With a zero size payload

   -----------------------
   | Type | Payload Size |
   -----------------------
   <- 4 -><----- 8 ------>

With a non-zero size payload

   -----------------------------------------
   | Type | Payload Size | Payload         |
   -----------------------------------------
   <- 4 -><----- 8 ------><- payload size -> (in bytes)

### Data Message Structure

Namespace and payload are required to be defined and valid.

  -----------------------------------------------------------------------------------
  | Type | NS len | NameSpace | blockID | Offset | Payload Size | Payload           |
  -----------------------------------------------------------------------------------
  <- 4 -><-- 8 ---><- NS len -><-- 8 --><-- 8 ---><---- 8 -----><-- payload size -->

## Connection Handshake

When a client connects to a server, a connection handshake occurs. The handshake allows us
to safely and correctly create a connection, as well as getting basic information from the
server upon connection, e.g., the block size supported by the server.
During a connection handshake, the client first sends a connection request. When the server
receives the connect request, it replies with a connection ACK message that include its block
size.
Both these message types are basic messages

### Connection Requests

A connection request is a basic message, that a client sends when connecting to a server:
  ---------------
  | CONNREQ | 0 |
  ---------------

### Connection Ack

A connection ACK is the response from a server to a client when receiving a connection request.

  ----------------------------
  | CONNACK | 8 | block size |
  ----------------------------

# Termination Messages

A termination message is a basic message that can be sent typically from the client to a server,
in order to finalize a connection.

   ---------------
   | TERMMSG | 0 |
   ---------------

Such a message could potentially use a paylaod, but the default implemnentation does not include one

# Data Messages

A data message can be exchanged from a client to a server (it hold all the data required to store the data
once received by a server) in the context of a write operation of the file system; or between the server and
a client in the context of a read operation.
In the context of a read operation, when the data is returned from the server, blockid is ignored by the
client.
