import time
import copy 
import json
import socket
import threading

# running locally
from message import Message



class Servidor:

    HOST = socket.gethostbyname(socket.gethostname())
    PORT = 10098
    ALIVE_PORT = PORT + 1
    SERVER = (HOST, PORT)
    BUFFERSIZE = 1024
    BROAD_CAST_TIME_INTERVAL = 5
    ALIVE_TIMEOUT = 3

    def __init__(self):
        self.UDPServerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDPServerSocket.bind((self.HOST, self.PORT))
        
        self.UDPAliveSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDPAliveSocket.bind((self.HOST, self.ALIVE_PORT))

        self.peers = {}  # Python Dict são thread safe

    def _receive(self):

        while True:
            try:
                data, peer_udp = self.UDPServerSocket.recvfrom(self.BUFFERSIZE)
                recv_msg = json.loads(data.decode('utf-8'))  # Transforma json em dict
                # Criar uma thread para cada requisição de um cliente
                thread = threading.Thread(
                    target=self._handle_request, args=(recv_msg,peer_udp))

                thread.start()
                thread.join()

            except KeyboardInterrupt:
                """
                No futuro eu devo tirar o client se algum erro acontecer
                """
                print()
                print("#" * 64)
                print(self.peers)
                print("#" * 64)
                self.UDPServerSocket.close()
                break

    def _handle_request(self, recv_msg, peer_udp):
        peer_tcp = tuple(recv_msg["sender"])  # Peer que fez a requisição
        msg_type = recv_msg["msg_type"]  # Tipo de requisição
        content = recv_msg["content"]  # Conteúdo da requisição

        if msg_type == "JOIN":
            return self._handle_join(peer_tcp, peer_udp, content)

        elif msg_type == "UPDATE":
            return self._handle_update(peer_tcp, peer_udp, content)
            
        elif msg_type == "SEARCH":
            return self._handle_search(peer_tcp, peer_udp, content)

        elif msg_type == "LEAVE":
            return self._handle_leave(peer_tcp, peer_udp)


    def _handle_join(self, peer_tcp, peer_udp, content):
        """
        Grava peer na estrutura de dados do servidor somente caso não esteja conectado.
        """
        file_lst = content.strip() # Retira possíveis espaços em branco do começo e final da string
        peer = (peer_tcp[0], (peer_tcp[1], peer_udp[1]))
        if peer not in self.peers:
            self.peers[peer] = file_lst.split()  # Grava o peer no servidor
            print(f"Peer [{peer_tcp[0]}]:[{peer_tcp[1]}] adicionado com arquivos {file_lst}")
            msg = Message(content=None, msg_type="JOIN_OK", sender=self.SERVER)
            self.UDPServerSocket.sendto(msg.to_json("utf-8"), peer_udp)
        else:
            msg = Message(content="Você já está conectado\n", msg_type="JOIN_OK", sender=self.SERVER)
            self.UDPServerSocket.sendto(msg.to_json("utf-8"), peer_udp)
            print(f"Peer [{peer_tcp[0]}]:{peer_tcp[1]} já está conectado.")

    def _handle_update(self, peer_tcp, peer_udp, new_file):

        peer = (peer_tcp[0], (peer_tcp[1], peer_udp[1]))
        if peer in self.peers:
            self.peers[peer].append(new_file.strip()) # Retira possíveis espaços em branco do começo e final da string e adiciona na estrutura
            msg = Message(content=None, msg_type="UPDATE_OK", sender=self.SERVER)
            self.UDPServerSocket.sendto(msg.to_json("utf-8"), peer_udp)
        #print(f"Informações do peer [{peer[1]}]:[{peer[0]}] atualizadas com sucesso.")

    def _handle_search(self, sender_peer, peer_udp, content):
        """
        Encontra quais peers tem o arquivo solicitado.
        """
        filename = content.strip() # 
        print(f"Peer [{sender_peer[0]}]:[{sender_peer[1]}] solicitou arquivo {filename}")
        has_peers = [f"{peer[0]}:{peer[1][0]}" for peer in self.peers if (filename in self.peers[peer]) and (sender_peer != (peer[0],peer[1][0]))]
        msg = Message(content="[" + " ".join(has_peers) + "]", 
                      msg_type="SEARCH",
                      sender=self.SERVER,
                      extra_info=filename)

        self.UDPServerSocket.sendto(msg.to_json("utf-8"), peer_udp)

    def _handle_leave(self, peer_tcp, peer_udp):

        peer = (peer_tcp[0], (peer_tcp[1], peer_udp[1]))
        if peer in self.peers:
            self.peers.pop(peer)  # Retira o peer do servidor
            msg = Message(content=None, msg_type="LEAVE_OK", sender=self.SERVER)
            self.UDPServerSocket.sendto(msg.to_json("utf-8"), peer_udp)

    def broadcast(self):
        """
        Envia requisição ALIVE para todos os peers a cada 'BROAD_CAST_TIME_INTERVAL' segundos.
        """
        
        msg = Message(content=None, msg_type="ALIVE", sender=self.SERVER, extra_info=(self.ALIVE_PORT))
        thread_alive = threading.Timer(self.BROAD_CAST_TIME_INTERVAL, self._broadcast_alive, args=[msg,])  # Inicializa uma Thread a cada 'BROAD_CAST_TIME_INTERVAL' segundos
        start_time = time.time()
        thread_alive.start()  # Inicializa a Thread
        thread_alive.join()  # Espera thread_alive terminar
        # print("--- %s seconds ---" % (time.time() - start_time))
        self.broadcast()

    def _broadcast_alive(self,msg):
        tmp_copy = copy.deepcopy(self.peers)
        for peer in tmp_copy:
            thread = threading.Thread(target=self._handle_alive, args=[msg, peer])
            thread.start()

    def _handle_alive(self, msg, peer):
        
        try:
            self.UDPAliveSocket.sendto(msg.to_json("utf-8"), (peer[0], peer[1][1]))
            self.UDPAliveSocket.settimeout(self.ALIVE_TIMEOUT)
            data, peer_udp = self.UDPAliveSocket.recvfrom(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))  # Transforma json em dict

        except socket.timeout:
            print(f"Peer [{peer[0]}]:[{peer[1][0]}] morto. Eliminando seus arquivos [{' '.join(self.peers[peer])}]")
            self.peers.pop(peer) 


if __name__ == "__main__":

    server = Servidor()
    listening_thread = threading.Thread(target=server._receive)
    alive_thread = threading.Thread(target=server.broadcast)

    listening_thread.start()
    alive_thread.start()

    # print("SERVER ON ...")
