//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2020 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CompressNIO
import CNIOExtrasZlib
import NIO
import NIOHTTP1

/// A HTTPResponseCompressor is a outbound channel handler that handles automatic streaming compression of
/// HTTP requests.
///
/// This compressor supports gzip and deflate. It works best if many writes are made between flushes.
///
/// Note that this compressor performs the compression on the event loop thread. This means that compressing
/// some resources, particularly those that do not benefit from compression or that could have been compressed
/// ahead-of-time instead of dynamically, could be a waste of CPU time and latency for relatively minimal
/// benefit. This channel handler should be present in the pipeline only for dynamically-generated and
/// highly-compressible content, which will see the biggest benefits from streaming compression.
public final class NIOHTTPRequestCompressor: ChannelOutboundHandler, RemovableChannelHandler {
    public typealias OutboundIn = HTTPClientRequestPart
    public typealias OutboundOut = HTTPClientRequestPart

    /// Handler state
    enum State {
        /// handler hasn't started
        case idle
        /// handler has recived a head
        case head(HTTPRequestHead)
        /// handler has received a head and a body, but hasnt written anything yet
        case body(HTTPRequestHead, ByteBuffer)
        /// handler has written the head and some of the body out.
        case partialBody(ByteBuffer)
        /// handler has finished
        case end
    }

    /// encoding algorithm to use
    var encoding: CompressionAlgorithm
    /// handler state
    var state: State
    /// compression handler
    var compressor: NIOCompressor
    /// pending write promise
    var pendingWritePromise: EventLoopPromise<Void>!
    
    /// Initialize a NIOHTTPRequestCompressor
    /// - Parameter encoding: Compression algorithm to use
    public init(encoding: CompressionAlgorithm) {
        self.encoding = encoding
        self.state = .idle
        self.compressor = encoding.compressor
        self.compressor.window = ByteBufferAllocator().buffer(capacity: 64*1024)
    }
    
    public func handlerAdded(context: ChannelHandlerContext) {
        pendingWritePromise = context.eventLoop.makePromise()
    }

    public func handlerRemoved(context: ChannelHandlerContext) {
        pendingWritePromise.fail(NIOCompression.Error.uncompressedWritesPending)
    }

    /// Write to channel
    /// - Parameters:
    ///   - context: Channel handle context which this handler belongs to
    ///   - data: Data being sent through the channel
    ///   - promise: The eventloop promise that should be notified when the operation completes
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        pendingWritePromise.futureResult.cascade(to: promise)
        do {
            let httpData = unwrapOutboundIn(data)
            switch httpData {
            case .head(let head):
                switch state {
                case .idle:
                    state = .head(head)
                default:
                    preconditionFailure("Unexpected HTTP head")
                }
                try compressor.startStream()

            case .body(let buffer):
                switch state {
                case .head(var head):
                    // We only have a head, this is the first body part
                    guard case .byteBuffer(let part) = buffer else { preconditionFailure("Expected a ByteBuffer") }
                    // now we have a body lets add the content-encoding header
                    head.headers.replaceOrAdd(name: "Content-Encoding", value: self.encoding.description)
                    state = .body(head, part)
                case .body(let head, var body):
                    // we have a head and a body, extend the body with this body part
                    guard case .byteBuffer(var part) = buffer else { preconditionFailure("Expected a ByteBuffer") }
                    body.writeBuffer(&part)
                    state = .body(head, body)
                case .partialBody(var body):
                    // we have a partial body, extend the partial body with this body part
                    guard case .byteBuffer(var part) = buffer else { preconditionFailure("Expected a ByteBuffer") }
                    body.writeBuffer(&part)
                    state = .partialBody(body)
                default:
                    preconditionFailure("Unexpected Body")
                }
                
            case .end:
                switch state {
                case .head(let head):
                    // only found a head
                    context.write(wrapOutboundOut(.head(head)), promise: nil)
                    context.write(data, promise: pendingWritePromise)
                case .body(var head, var body):
                    // have head and the whole of the body. Compress body, set content length header and write it all out, including the end
                    let outputBuffer = try body.compressStream(with: compressor, flush: .finish, allocator: context.channel.allocator)
                    head.headers.replaceOrAdd(name: "Content-Length", value: outputBuffer.readableBytes.description)
                    context.write(wrapOutboundOut(.head(head)), promise: nil)
                    context.write(wrapOutboundOut(.body(.byteBuffer(outputBuffer))), promise: nil)
                    context.write(data, promise: pendingWritePromise)
                case .partialBody(var body):
                    // have a section of the body. Compress that section of the body and write it out along with the end
                    try body.compressStream(with: compressor, flush: .finish) { buffer in
                        context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: nil)
                    }
                    context.write(data, promise: pendingWritePromise)
                default:
                    preconditionFailure("Unexpected End")
                }
                state = .end
                try compressor.finishStream()
            }
        } catch {
            pendingWritePromise.fail(error)
        }
    }
    
    public func flush(context: ChannelHandlerContext) {
        do {
            switch state {
            case .head(var head):
                // given we are flushing the head now we have to assume we have a body and set Content-Encoding
                head.headers.replaceOrAdd(name: "Content-Encoding", value: self.encoding.description)
                head.headers.remove(name: "Content-Length")
                head.headers.replaceOrAdd(name: "Transfer-Encoding", value: "chunked")
                context.write(wrapOutboundOut(.head(head)), promise: pendingWritePromise)
                pendingWritePromise = nil
                state = .partialBody(context.channel.allocator.buffer(capacity: 0))

            case .body(var head, var body):
                // Write out head with transfer-encoding set to "chunked" as we cannot set the content length
                // Compress and write out what we have of the the body
                head.headers.remove(name: "Content-Length")
                head.headers.replaceOrAdd(name: "Transfer-Encoding", value: "chunked")
                context.write(wrapOutboundOut(.head(head)), promise: nil)
                try body.compressStream(with: compressor, flush: .no) { buffer in
                    context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: pendingWritePromise)
                    pendingWritePromise = nil
                }
                state = .partialBody(context.channel.allocator.buffer(capacity: 0))
                
            case .partialBody(var body):
                // Compress and write out what we have of the body
                try body.compressStream(with: compressor, flush: .no) { buffer in
                    context.write(wrapOutboundOut(.body(.byteBuffer(buffer))), promise: pendingWritePromise)
                    pendingWritePromise = nil
                }
                state = .partialBody(context.channel.allocator.buffer(capacity: 0))
                
            default:
                context.flush()
                return
            }
            // reset pending write promise
            if pendingWritePromise == nil {
                pendingWritePromise = context.eventLoop.makePromise()
            }
            context.flush()
        } catch {
            pendingWritePromise.fail(error)
        }
    }
}

