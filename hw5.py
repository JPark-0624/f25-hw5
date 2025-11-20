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

    logger = homework5.logging.get_logger("hw5-sender")

    # Packet format:
    #   type (1 byte): 0=DATA, 1=ACK, 2=FIN, 3=FINACK
    #   seq (4 bytes): byte offset for DATA/FIN packets
    #   ack (4 bytes): next expected byte for ACK/FINACK packets
    #   length (2 bytes): length of payload that follows
    header_struct = struct.Struct("!BIIH")
    header_size = header_struct.size
    max_payload = homework5.MAX_PACKET - header_size

    DATA, ACK, FIN, FINACK = 0, 1, 2, 3

    def build_packet(pkt_type: int, seq: int = 0, ack_num: int = 0, payload: bytes = b"") -> bytes:
        return header_struct.pack(pkt_type, seq, ack_num, len(payload)) + payload

    def parse_packet(raw: bytes):
        if len(raw) < header_size:
            return None
        pkt_type, seq_num, ack_num, length = header_struct.unpack(raw[:header_size])
        payload = raw[header_size:header_size + length]
        return pkt_type, seq_num, ack_num, payload

    # RTT estimation variables (TCP-like)
    srtt = None
    rttvar = None
    min_rto = 0.1   # 매우 공격적인 최소값
    max_rto = 1.0   # 더 낮은 최대값
    rto_backoff = 1.0

    def base_rto() -> float:
        if srtt is None:
            return 0.5  # 초기값을 0.5초로 줄임
        return max(min_rto, min(max_rto, srtt + 4 * rttvar))

    def current_rto() -> float:
        return min(max_rto, base_rto() * rto_backoff)

    window_size = 2  # channel allows only two packets in flight
    base = 0  # earliest unacked byte
    next_seq = 0
    unacked: typing.Dict[int, typing.Tuple[float, bytes]] = {}

    sock.settimeout(current_rto())

    def send_data_packet(seq: int, payload: bytes):
        pkt = build_packet(DATA, seq, 0, payload)
        sock.send(pkt)
        unacked[seq] = (time.time(), payload)
        logger.debug("Sent DATA seq=%d len=%d", seq, len(payload))

    # Main loop to transmit data and react to ACKs/timeouts
    while base < len(data) or unacked:
        # Fill the window
        while len(unacked) < window_size and next_seq < len(data):
            chunk = data[next_seq:next_seq + max_payload]
            send_data_packet(next_seq, chunk)
            next_seq += len(chunk)

        try:
            sock.settimeout(current_rto())
            incoming = sock.recv(homework5.MAX_PACKET)
            if not incoming:
                continue
            parsed = parse_packet(incoming)
            if not parsed:
                continue
            pkt_type, seq_num, ack_num, payload = parsed

            if pkt_type == ACK:
                if ack_num > base:
                    # Successful progress resets backoff
                    rto_backoff = 1.0
                    # Update RTT using the oldest newly acknowledged packet
                    acked_seqs = [s for s in unacked.keys() if s < ack_num]
                    for s in sorted(acked_seqs):
                        sent_time, _ = unacked.pop(s)
                        sample_rtt = time.time() - sent_time
                        if srtt is None:
                            srtt = sample_rtt
                            rttvar = sample_rtt / 2
                        else:
                            alpha, beta = 0.125, 0.25
                            rttvar = (1 - beta) * rttvar + beta * abs(srtt - sample_rtt)
                            srtt = (1 - alpha) * srtt + alpha * sample_rtt
                    base = ack_num
                elif ack_num == base and unacked:
                    # Duplicate ACK - 중복 ACK는 패킷 손실 신호
                    # 가장 오래된 패킷 즉시 재전송
                    logger.debug("Duplicate ACK %d", ack_num)
                    oldest_seq = min(unacked.keys())
                    _, payload = unacked[oldest_seq]
                    pkt = build_packet(DATA, oldest_seq, 0, payload)
                    sock.send(pkt)
                    unacked[oldest_seq] = (time.time(), payload)
            elif pkt_type == FINACK:
                # Receiver confirmed completion
                unacked.clear()
                base = len(data)
                break
        except socket.timeout:
            # Retransmit oldest unacked packet with exponential backoff
            if unacked:
                oldest_seq = min(unacked.keys())
                _, payload = unacked[oldest_seq]
                pkt = build_packet(DATA, oldest_seq, 0, payload)
                sock.send(pkt)
                unacked[oldest_seq] = (time.time(), payload)
                rto_backoff = min(2.0, rto_backoff * 1.25)  # 더 완만한 백오프
                logger.debug("Timeout -> resend DATA seq=%d (backoff=%0.2f)", oldest_seq, rto_backoff)
            continue

    # All data acknowledged, initiate teardown
    fin_seq = len(data)
    fin_packet = build_packet(FIN, fin_seq, 0)
    fin_attempts = 0
    max_fin_attempts = 10
    
    while fin_attempts < max_fin_attempts:
        try:
            sock.settimeout(current_rto())
            sock.send(fin_packet)
            logger.debug("Sent FIN seq=%d (attempt %d)", fin_seq, fin_attempts + 1)
            resp = sock.recv(homework5.MAX_PACKET)
            parsed = parse_packet(resp)
            if parsed and parsed[0] == FINACK:
                # Send a final ACK
                ack_pkt = build_packet(ACK, 0, parsed[2])
                sock.send(ack_pkt)
                logger.debug("Received FINACK, sent final ACK")
                break
        except socket.timeout:
            fin_attempts += 1
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

    header_struct = struct.Struct("!BIIH")
    header_size = header_struct.size
    DATA, ACK, FIN, FINACK = 0, 1, 2, 3

    def build_packet(pkt_type: int, seq: int = 0, ack_num: int = 0, payload: bytes = b"") -> bytes:
        return header_struct.pack(pkt_type, seq, ack_num, len(payload)) + payload

    expected_seq = 0
    buffer: typing.Dict[int, bytes] = {}
    total_written = 0
    last_activity = time.time()
    idle_timeout = 3.0  # 3초로 줄임

    sock.settimeout(0.3)  # 0.3초로 짧게 - ACK 손실 대비

    while True:
        try:
            packet = sock.recv(homework5.MAX_PACKET)
            if not packet:
                continue
            
            last_activity = time.time()
            
            if len(packet) < header_size:
                continue
            pkt_type, seq_num, ack_num, length = header_struct.unpack(packet[:header_size])
            payload = packet[header_size:header_size + length]

            if pkt_type == DATA:
                if seq_num == expected_seq:
                    dest.write(payload)
                    total_written += len(payload)
                    expected_seq += len(payload)
                    dest.flush()
                    # Deliver buffered contiguous data
                    while expected_seq in buffer:
                        chunk = buffer.pop(expected_seq)
                        dest.write(chunk)
                        total_written += len(chunk)
                        expected_seq += len(chunk)
                        dest.flush()
                elif seq_num > expected_seq and seq_num not in buffer:
                    buffer[seq_num] = payload
                # Always ACK current expectation
                ack_pkt = build_packet(ACK, 0, expected_seq)
                sock.send(ack_pkt)
                logger.debug("ACK %d", expected_seq)
            elif pkt_type == FIN:
                # Send FINACK containing last byte received
                finack = build_packet(FINACK, seq_num, expected_seq)
                sock.send(finack)
                logger.debug("FIN received, sent FINACK ack=%d", expected_seq)
                
                # Wait briefly for final ACK, then exit
                try:
                    sock.settimeout(0.2)  # 0.2초로 짧게
                    final = sock.recv(homework5.MAX_PACKET)
                    if final:
                        logger.debug("Received final packet after FINACK")
                except socket.timeout:
                    pass
                break
            elif pkt_type == ACK:
                # Sender may send a last ACK after FINACK; ignore
                continue
        except socket.timeout:
            # Check if we've been idle too long
            if time.time() - last_activity > idle_timeout:
                logger.debug("Idle timeout - assuming transfer complete")
                break
            
            # In case ACKs are lost, retransmit latest ACK only after data has arrived
            if expected_seq > 0:
                ack_pkt = build_packet(ACK, 0, expected_seq)
                sock.send(ack_pkt)
            continue

    return total_written
