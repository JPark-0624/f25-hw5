"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """

    # Naive implementation where we chunk the data to be sent into
    # packets as large as the network will allow, and then send them
    # over the network, pausing half a second between sends to let the
    # network "rest" :)
    logger = homework5.logging.get_logger("hw5-sender")

    
    DATA = 0
    ACK = 1
    FIN = 2
    FINACK = 3

    header_struct = struct.Struct("!BIIH")
    
    alpha = 0.125
    beta = 0.25
    min_rto = 0.1
    max_rto = 1.0
    rto_backoff = 1.0

    
    EstimatedRTT = None #(1-alpha )*EstimatedRTT + alpha*SampleRTT
    DevRTT = None #(1-beta) * DevRTT + beta * abs(SampleRTT - EstimatedRTT)
    
    # base_rto = max(min_rto, min(max_rto, EstimatedRTT + 4 * DevRTT))
    current_rto = min(max_rto, 0.5 * rto_backoff)

    sock.settimeout(current_rto)



    window = 2
    buffer = 0
    nextseq = 0
    unacked: typing.Dict[int, typing.Tuple[float,bytes]] = {}
    max_payload = homework5.MAX_PACKET - header_struct.size

    while buffer < len(data) or unacked:
        while len(unacked) < window and nextseq < len(data):
            chunk = data[nextseq:nextseq + max_payload]
            packet = header_struct.pack(DATA, nextseq,0,len(chunk)) + chunk
            sock.send(packet)
            unacked[nextseq] = (time.time(), chunk)
            nextseq += len(chunk)

        try:
            sock.settimeout(current_rto)
            raw = sock.recv(homework5.MAX_PACKET)
            if not raw:
                continue
            if len(raw) < header_struct.size:
                continue
            pkt_type, seq_num, ack_num, length = header_struct.unpack(raw[:header_struct.size])
            payload = raw[header_struct.size:header_struct.size + length]
            
            if pkt_type == ACK:
                if ack_num > buffer:
                    rto_backoff = 1.0
                    acked_sequence = [seq for seq in unacked.keys() if seq < ack_num]
                    for seq in sorted(acked_sequence):
                        send_time, _ = unacked.pop(seq)
                        sample_rtt = time.time() - send_time
                        # EstimatedRTT = (1 - alpha) * EstimatedRTT + alpha * sample_rtt
                        if EstimatedRTT is None:
                            EstimatedRTT = sample_rtt
                            DevRTT = EstimatedRTT / 2
                        else:
                            DevRTT = (1 - beta) * DevRTT + beta * abs(sample_rtt - EstimatedRTT)
                            EstimatedRTT = (1-alpha )*EstimatedRTT + alpha*sample_rtt
                    buffer = ack_num
                elif ack_num == buffer and unacked:
                    logger.debug("Duplicate ACK %d", ack_num)
                    oldest_seq = min(unacked.keys())
                    _, payload = unacked[oldest_seq]
                    packet = header_struct.pack(DATA, oldest_seq,0,len(payload)) + payload
                    sock.send(packet)
                    unacked[oldest_seq] = (time.time(), payload)
            elif pkt_type == FINACK:
                unacked.clear()
                buffer = len(data)
                break
        except socket.timeout:
            if unacked:
                oldest_seq = min(unacked.keys())
                _, payload = unacked[oldest_seq]
                header_struct.pack(DATA, oldest_seq, 0, len(payload)) + payload
                sock.send(packet)
                unacked[oldest_seq] = (time.time(), payload)
                rto_backoff = min(2.0, rto_backoff * 1.25)
            continue

    fin_seq = len(data)
    fin_packet = header_struct.pack(FIN, fin_seq, 0, 0)
    sock.send(fin_packet)
    fin_attemps = 0
    fin_attemps_limit = 10

    while fin_attemps < fin_attemps_limit:
        try:
            sock.settimeout(current_rto)
            sock.send(fin_packet)
            resp = sock.recv(homework5.MAX_PACKET)
            parsed = header_struct.unpack(resp)
            if parsed and parsed[0] == FINACK:
                ack_pkt = header_struct.pack(ACK, 0, parsed[2], 0)
                sock.send(ack_pkt)
                logger.debug("Received FINACK, sent final ACK")
                break
        except socket.timeout:
            fin_attemps += 1
            continue

def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    logger = homework5.logging.get_logger("hw5-receiver")
    # Naive solution, where we continually read data off the socket
    # until we don't receive any more data, and then return.
    DATA = 0
    ACK = 1
    FIN = 2
    FINACK = 3
    header_struct = struct.Struct("!BIIH")

    expected_seq = 0
    buffer: typing.Dict[int, bytes] = {}
    total_bytes = 0
    last_time = time.time()
    timeout = 3.0

    sock.settimeout(timeout*0.1)

    while True:
        try:
            packet = sock.recv(homework5.MAX_PACKET)
            if not packet:
                continue
            
            last_time = time.time()
            
            if len(packet) < header_struct.size :
                continue
            pkt_type, seq_num, ack_num, length = header_struct.unpack(packet[:header_struct.size]) 
            payload = packet[header_struct.size:header_struct.size + length]

            if pkt_type == DATA:
                if seq_num == expected_seq:
                    dest.write(payload)
                    total_bytes += len(payload)
                    expected_seq += len(payload)
                    dest.flush()
                    while expected_seq in buffer:
                        chink = buffer.pop(expected_seq)
                        dest.write(chink)
                        total_bytes += len(chink)
                        expected_seq += len(chink)
                        dest.flush()
                elif seq_num > expected_seq and seq_num not in buffer:
                    buffer[seq_num] = payload
                ack_pkt = header_struct.pack(ACK, 0, expected_seq, 0)
                sock.send(ack_pkt)

            elif pkt_type == FIN:
                finack_pkt = header_struct.pack(FINACK, 0, seq_num + length, 0)
                sock.send(finack_pkt)
                try:
                    sock.settimeout(timeout*0.07)
                    final = sock.recv(homework5.MAX_PACKET)
                    
                except socket.timeout:
                    pass
                break
            elif pkt_type == ACK:
                continue
        except socket.timeout:
            if time.time() - last_time > timeout:
                break
    
            if expected_seq >0 :
                ack_pkt = header_struct.pack(ACK, 0, expected_seq, 0)
                sock.send(ack_pkt)
            continue

    return total_bytes