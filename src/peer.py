# pacotes default do python
import os   # Utilitys do sistema operacional
import sys  # Para pegar folder no qual os dados estão
import glob  # Percorre com regex os files de um folder
import json  # Utilizado para fazer parse da mensagem
from collections import defaultdict # Utilizado como estrutura de dados de um peer para guardar quais peers possuem quais arquivos
import random  # Utilizado para aceitar randomicamente requisição de download
import time  # Utilizado para ajustar prints de um peer 
import socket  # Utilizado para criar sockets
import threading # Utilizado para fazer as threads
from typing import List, Dict, Tuple  # Utilizado para indicar tipos de variáveis

# pacotes não default
# from tqdm import tqdm  # Utilizado para fazer barra de progresso do download
from message import Message


class Peer:

    PEER_ADRESS = socket.gethostbyname(socket.gethostname())

    SERVER_ADDRESS = "127.0.1.1"
    SERVER_PORT = 10098
    SERVER = (SERVER_ADDRESS, SERVER_PORT)
    BUFFERSIZE = 4096
    PROP_ACCEPT = 0.5  # Probabilidade do peer aceitar o download

    def __init__(self, file_folder_path, port):

        # Define peer
        self.PEER_PORT = int(port)
        self.PEER = (self.PEER_ADRESS, self.PEER_PORT)

        # Cria socket TCP
        self.TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.TCPSocket.bind(self.PEER) 
        self.TCPSocket.listen()

        # Cria socket UDP
        self.UDPClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Variaveis auxiliares
        self.network_peers = defaultdict(lambda:[]) # Guarda quais arquivos cada peer tem
        self.file_folder_path:str = file_folder_path # Guarda pasta onde os arquvios de áudio estão.
        files_path = [file_name for file_name in glob.glob(f"{file_folder_path}/*")] # Lista todos os arquivos da pasta
        self.files:str = " ".join([os.path.basename(file_name) for file_name in files_path]) # String contendo arquivo para transmissão de informação
        self.menu_str:str = "\nDigite a requisição [JOIN, SEARCH, DOWNLOAD, LEAVE]:"

    def _handle_download(self):
        while True:
            sender_socket, _ = self.TCPSocket.accept() 
            # if below code is executed, that means the sender is connected
            data = sender_socket.recv(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))
            requested_file = recv_msg["content"]

            file_path = os.path.join(self.file_folder_path, requested_file)
            thread = threading.Thread(target=self._receive_download, args=(file_path, sender_socket,))
            thread.start()
            thread.join()

    def _receive_download(self, file_path, sender_socket):

        filesize = os.path.getsize(file_path)
        chance_accept = random.uniform(0,1)
        if chance_accept <= self.PROP_ACCEPT:
            msg = Message(content=None, msg_type="DOWNLOAD_ACEITO", sender=self.PEER, extra_info=filesize)
            sender_socket.sendall(msg.to_json("utf-8"))
            #with tqdm(range(filesize), f"Sending {requested_file}", unit="B", unit_scale=True, unit_divisor=1024) as progress_bar:
            with open(file_path, "rb") as f:
                while True:
                    bytes_read = f.read(self.BUFFERSIZE)  # Lê bytes do arquivo
                    if not bytes_read: 
                        break # Transmissão do arquivo completa

                    sender_socket.sendall(bytes_read)
                    # progress_bar.update(len(bytes_read))
        else:
            msg = Message(content=None, msg_type="DOWNLOAD_NEGADO", sender=self.PEER, extra_info=filesize)
            sender_socket.sendall(msg.to_json("utf-8"))

        sender_socket.close()

    
            
    def _receive(self):
        while True:
            data, _ = self.UDPClientSocket.recvfrom(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))  # Transforma json em dict
            thread = threading.Thread(target=self._handle_request, args=(recv_msg,))
            thread.start()
            thread.join()

    def _handle_request(self, recv_msg):

        msg_type = recv_msg["msg_type"]  # Tipo de requisição
        content = recv_msg["content"]  # Conteúdo da requisição
        extra = recv_msg["extra_info"]  # Conteúdo extra utilizado por algumas requisições

        if msg_type == "JOIN_OK":
            self._handle_join()

        elif msg_type == "UPDATE_OK":
            self._handle_update()

        elif msg_type == "LEAVE_OK":
            self._handle_leave()

        elif msg_type == "ALIVE":
            return self._handle_alive(extra)

        elif msg_type == "SEARCH":
            self._handle_search(content, extra)

    def _handle_join(self):
        print(f"Sou o peer [{self.PEER_ADRESS}]:[{self.PEER_PORT}] com arquivos {self.files}\n")

    def _handle_update(self):
        pass
        #print("Informações atualizadas com sucesso.")

    def _handle_search(self, content, filename):
        parse_msg = content.strip('[]').split()
        for peer_str in parse_msg:
            address, port = peer_str.split(':')
            self.network_peers[(address, int(port))].append(filename)
        print(f"Peers com arquivo solicitado: {content}")

    def _handle_leave(self):
        sys.exit("Desconectado")

    def _handle_alive(self, alive_port):
        msg = Message(content=None, msg_type="ALIVE_OK", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), (self.SERVER_ADDRESS, alive_port))

    def _request(self):
        while True:
            time.sleep(0.1)
            request = input(self.menu_str)
            thread = threading.Thread(target=self._handle_write, args=(request,))
            thread.start()
            thread.join()

    def _handle_write(self, request):
        command, *msg = request.split()
        command = command.upper()

        if command == "JOIN":
            self.join()

        elif command == "SEARCH":
            msg_len = len(msg)
            if msg_len == 0: print("Faltou o nome do arquivo.")
            elif msg_len == 1: self.search(msg[0])
            else: print("Somente 1 arquivo por vez.")

        elif command == "DOWNLOAD":
            msg_len = len(msg)
            if msg_len == 0: print("Faltou o nome do arquivo.")
            elif msg_len == 1: self.download(msg[0])
            else: print("Somente 1 arquivo por vez.")

        elif command == "LEAVE":
            self.leave()
        else:
            print("Comando Invalido.")

    def join(self):
        msg = Message(content=self.files, msg_type="JOIN", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

    def search(self, requested_file):
        msg = Message(content=requested_file, msg_type="SEARCH", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

    def download(self, requested_file):

        new_file_path = os.path.join(self.file_folder_path, requested_file)

        # if os.path.exists(new_file_path):
        #    print("Você ja possui esse arquivo.")
        #    return None

        for peer in self.network_peers:
            if requested_file in self.network_peers[peer]:

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(peer)
                msg = Message(content=requested_file, msg_type="DOWNLOAD", sender=self.PEER)
                print(f"Pedindo arquivo para o Peer [{peer[0]}]:[{peer[1]}]")
                s.send(msg.to_json("utf-8"))

                # Recebe via TCP do outro peer se o download foi aceito e tamanho do arquivo solicitado
                info_downlaod = s.recv(self.BUFFERSIZE).decode("utf-8")
                answer_download = json.loads(info_downlaod)
                filesize = int(answer_download["extra_info"])
                msg_type = answer_download["msg_type"]
                time.sleep(0.1)
                if msg_type == "DOWNLOAD_ACEITO":
                    #with tqdm(range(filesize), f"Receiving {requested_file}", unit="B", unit_scale=True, unit_divisor=1024) as progress_bar:
                    with open(new_file_path, "wb") as f:
                        while True:
                            bytes_read = s.recv(self.BUFFERSIZE)
                            if not bytes_read:
                                break
                            f.write(bytes_read)
                    #            progress_bar.update(len(bytes_read))

                    print(f"Arquivo {requested_file} baixado com sucesso na pasta {self.file_folder_path}")

                    msg = Message(content=requested_file, msg_type="UPDATE", sender=self.PEER)
                    self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

                    return None

                elif msg_type == "DOWNLOAD_NEGADO":
                    print(f"Peer [{peer[0]}]:[{peer[1]}] negou o download.")

                s.close()

    def leave(self):
        msg = Message(content=None, msg_type="LEAVE", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)
        os._exit(os.EX_OK)


if __name__ == "__main__":

    # Pega porta TCP do peer e folder no quais os arquivos de vídeo estão
    _, file_folder_path, port_tcp = sys.argv

    # Inicializa o peer
    peer = Peer(file_folder_path, port_tcp)

    # Defino o peer pela sua porta TCP
    # print(f"Peer ONLINE:\nIP:{peer.PEER_ADRESS}\tPORT:{peer.PEER_PORT}")

    # Iniciliza thread
    download_thread = threading.Thread(target=peer._handle_download) # Responsável pelas requisições TCP de DOWNLOAD
    listening_thread = threading.Thread(target=peer._receive) # Responsável por qualquer requisição UDP
    request_thread = threading.Thread(target=peer._request) # Responsável por fazer requisições

    # Start as thread 
    download_thread.start()
    listening_thread.start()
    request_thread.start()
