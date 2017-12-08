### WebSockets, SocketIO, and Flask-SocketIO

## Contents
- What are WebSockets?
- SocketIO
- Flask-SocketIO
- Examples

## What are WebSockets?

WebSockets are one of several communication protocols for transmitting data (the
most well-known probably being HTTP). WebSockets enable bi-directional, full duplex
communication between a server and client (or clients) through a single TCP socket
connection. As a full-duplex standard, WebSockets allow both the server and the
client to communicate simultaneously. This is in contrast to half-duplex systems
like HTTP where the communication is one direction at a time.

Resources:

- PubNub: WebSockets vs REST: https://www.pubnub.com/blog/2015-01-05-websockets-vs-rest-api-understanding-the-difference/
- How WebSockets Work - With Socket.io Demo: https://thesocietea.org/2016/04/how-websockets-work-with-socket-io-demo/

## SocketIO

SocketIO is the most popular library for enabling sockets for JavaScript/Node.JS,
Java, PHP, and .NET. We will use this library for writing the JavaScript and Java
clients to our Python Flask server.

Resources:
- Official docs: https://socket.io/
- An Intro to Socket.io: https://divillysausages.com/2015/07/12/an-intro-to-socket-io/
    - Godsend guide that fills in the gaps left by the sparse official documentation.

## Flask-SocketIO

Flask-SocketIO implements the message passing protocol exposed by the SocketIO
JavaScript library for Flask applications.

Resources:

- Official docs: http://flask-socketio.readthedocs.io/en/latest/

## Examples

See example project: https://github.com/stellarluna/flask_socket_example
