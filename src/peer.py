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
#from tqdm import tqdm  # Utilizado para fazer barra de progresso do download
from message import Message


class Peer:

    PEER_ADRESS = socket.gethostbyname(socket.gethostname())

    SERVER_ADDRESS = "127.0.0.1"
    SERVER_PORT = 10098
    SERVER = (SERVER_ADDRESS, SERVER_PORT)
    BUFFERSIZE = 4096
    PROP_ACCEPT = 0.5  # Probabilidade do peer aceitar o download
    REQUEST_TIMEOUT = 3
    QTD_RETRY = 2

    def __init__(self, file_folder_path):

        # Cria socket TCP para requisição de DOWNLOAD
        self.TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.TCPSocket.bind((self.PEER_ADRESS, 0))  # Delega pegar uma porta ao SO
        self.TCPSocket.listen()
        self.TCP_PORT = self.TCPSocket.getsockname()[1]

        # Cria socket UDP para requisições JOIN, SEARCH, LEAVE e UPDATE
        self.UDPClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDPClientSocket.bind((self.PEER_ADRESS, 0))  # Delega pegar uma porta ao SO
        self.UDP_PORT = self.UDPClientSocket.getsockname()[1]

        # Cria socket UDP para receber ALIVE
        self.UDPAliveClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.UDPAliveClientSocket.bind((self.PEER_ADRESS, 0))  # Delega pegar uma porta ao SO
        self.UDP_ALIVE_PORT = self.UDPAliveClientSocket.getsockname()[1]
        # Define o peer
        self.PEER = (self.PEER_ADRESS, (self.TCP_PORT, self.UDP_PORT, self.UDP_ALIVE_PORT))

        # Variaveis auxiliares
        self.qtd_try = 0  # Indica quantas vezes uma mesma requisição UDP foi chamada.
        self.network_peers = {} # Estrutura de dados que guarda quais peers tem o arquivo solicitado
        self.file_folder_path:str = file_folder_path # Guarda pasta onde os arquvios de áudio estão.
        files_path = [file_name for file_name in glob.glob(f"{file_folder_path}/*")] # Lista todos os arquivos da pasta
        self.files:str = " ".join([os.path.basename(file_name) for file_name in files_path]) # String contendo arquivo para transmissão de informação
        self.menu_str:str = "\nDigite a requisição [JOIN, SEARCH, DOWNLOAD, LEAVE]:"

    def _listen_download(self):
        """
        Ouve por connects de outros peers.
        Após conexão cria uma thread que executa a função que lida com o download.
        """
        while True:
            sender_socket, _ = self.TCPSocket.accept() 
            data = sender_socket.recv(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))
            requested_file = recv_msg["content"]

            file_path = os.path.join(self.file_folder_path, requested_file)
            thread = threading.Thread(target=self._handle_download, args=(file_path, sender_socket,))
            thread.start()
            thread.join()

    def _handle_download(self, file_path, sender_socket):
        """
        Aceita aleotariamente os pedidos dos peers. 
        Caso aceito envia arquivo por chunks ao peer que solicitante.
        """
        
        chance_accept = random.uniform(0,1)  # Chance de permitir download
        if chance_accept <= self.PROP_ACCEPT:
            filesize = os.path.getsize(file_path)  # Tamanho do arquivo a ser enviado
            msg = Message(content=None, msg_type="DOWNLOAD_ACEITO", sender=self.PEER, extra_info=filesize)
            sender_socket.sendall(msg.to_json("utf-8"))  # Envia tamanho do arquivo em bytes.
            #with tqdm(range(filesize), f"Sending {requested_file}", unit="B", unit_scale=True, unit_divisor=1024) as progress_bar:
            with open(file_path, "rb") as f:
                while True:
                    bytes_read = f.read(self.BUFFERSIZE)  # Lê bytes do arquivo.
                    if not bytes_read: 
                        break # Transmissão do arquivo completa.

                    sender_socket.sendall(bytes_read)
                    # progress_bar.update(len(bytes_read))
        else:
            msg = Message(content=None, msg_type="DOWNLOAD_NEGADO", sender=self.PEER)
            sender_socket.sendall(msg.to_json("utf-8"))

        sender_socket.close()

    def _listen_alive(self):
        while True:
            data, _ = self.UDPAliveClientSocket.recvfrom(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))  # Transforma json em dict
            alive_port = int(recv_msg['extra_info'])
            thread = threading.Thread(target=self._handle_alive, args=(alive_port,)) # Faz o dispatch da requisição.
            thread.start()

    def _handle_alive(self, alive_port):
        """
        Manda uma mensagem ao servidor pela porta alive_port.
        """
        
        msg = Message(content=None, msg_type="ALIVE_OK", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), (self.SERVER_ADDRESS, alive_port))


    def _receive(self, command, msg):
        try:
            data, _ = self.UDPClientSocket.recvfrom(self.BUFFERSIZE)
            recv_msg = json.loads(data.decode('utf-8'))  # Transforma json em dict
            thread = threading.Thread(target=self._handle_request, args=(recv_msg,)) # Faz o dispatch da requisição.
            thread.start()
            thread.join()
            self.qtd_try = 0  # Set 0 pois a mensagem foi recebida

        except socket.timeout:
            if self.qtd_try < self.QTD_RETRY:
                self.qtd_try += 1
                # print(f"RETRY:{self.qtd_try} requisição: {command}")
                self.pipeline_request(command, msg)
            else:
                # res = input("Parece que o servidor morreu, deseja sair (y|n)?")
                # if res.lower() == "y":
                #    self._handle_leave()
                self.qtd_try = 0

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

        elif msg_type == "SEARCH":
            self._handle_search(content, extra)
        
        elif msg_type == "UNKNOWN":
            pass
            # print(content)

    def _handle_join(self):
        print(f"Sou o peer [{self.PEER_ADRESS}]:[{self.TCP_PORT}] com arquivos {self.files}\n")

    def _handle_update(self):
        pass
        #print("Informações atualizadas com sucesso.")

    def _handle_search(self, content, filename):
        """
        Atualiza a rede do peer, dessa forma sabe-se quais peers tem o arquivo solicitado.
        """

        self.network_peers = {}  # Limpa a estrutura de busca 
        parse_msg = content.strip('[]').split()  # Parse do conteúdo da mensagem
        for peer_str in parse_msg:
            address, port = peer_str.split(':')  # Parse do peer no formato string
            self.network_peers[(address, int(port))] = filename  # Adiciona o peer na rede se não existir e o arquivo especificado
        print(f"Peers com arquivo solicitado: {content}")

    def _handle_leave(self):
        """
        Desliga o peer.
        """
        os._exit(os.EX_OK)

    def _request(self):
        """
        Solicita ao usuário uma requisição e faz o dispatch de uma thread com o pedido.
        """
        while True:
            time.sleep(0.1) # Dorme 0.1 segundos para o print aparecer corretamente na tela. 
            request = input(self.menu_str)  # Printa o menu e solicita requisição ao usuario.

            command, *msg = request.split()
            command = command.upper()
            if command != "DOWNLOAD":
                self.UDPClientSocket.settimeout(self.REQUEST_TIMEOUT)  # Start o timeout somente após saber qual requisição será pedida.
            self.pipeline_request(command, msg)
            self.UDPClientSocket.settimeout(None)  # Zera timeout para o socket

    def pipeline_request(self, command, msg):
        
            receive_thread = threading.Thread(target=self._receive, args=(command, msg))  # Espera resposta para a requisição 
            request_thread = threading.Thread(target=self._dispatch_request, args=(command, msg))  # Faz o dispatch da requisição.

            request_thread.start()
            receive_thread.start()
            
            if command != "DOWNLOAD":
                receive_thread.join()

            request_thread.join()


    def _dispatch_request(self, command, msg):

        if command == "JOIN":
            self.join()

        elif command == "SEARCH":
            try:
                self.search(msg[0])
            except IndexError:
                self.wrong_msg()
  
        elif command == "DOWNLOAD":
            try:
                self.download(msg[0])
            except IndexError:
                self.wrong_msg()

        elif command == "LEAVE":
            self.leave()

        else:
            self.wrong_msg()

    def join(self):
        msg = Message(content=self.files, msg_type="JOIN", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

    def search(self, requested_file):
        msg = Message(content=requested_file, msg_type="SEARCH", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

    def leave(self):
        msg = Message(content=None, msg_type="LEAVE", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

    def download(self, requested_file):

        new_file_path = os.path.join(self.file_folder_path, requested_file)

        for peer in self.network_peers:
            if requested_file in self.network_peers[peer]:

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    s.connect(peer)
                except ConnectionRefusedError:
                    print("Peer não está disponível")
                    return None
                finally:
                    s.close
                msg = Message(content=requested_file, msg_type="DOWNLOAD", sender=self.PEER)
                print(f"Pedindo arquivo para o Peer [{peer[0]}]:[{peer[1]}]")
                s.send(msg.to_json("utf-8"))

                # Recebe via TCP do outro peer se o download foi aceito e tamanho do arquivo solicitado
                info_downlaod = s.recv(self.BUFFERSIZE).decode("utf-8")
                answer_download = json.loads(info_downlaod)
                msg_type = answer_download["msg_type"]
                time.sleep(0.1)
                if msg_type == "DOWNLOAD_ACEITO":
                    filesize = int(answer_download["extra_info"])
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

    def wrong_msg(self):
        msg = Message(content="", msg_type="UNKNOWN", sender=self.PEER)
        self.UDPClientSocket.sendto(msg.to_json("utf-8"), self.SERVER)

if __name__ == "__main__":

    # Pega porta TCP do peer e folder no quais os arquivos de vídeo estão
    _, file_folder_path = sys.argv

    # Inicializa o peer
    peer = Peer(file_folder_path)

    # Iniciliza thread
    download_thread = threading.Thread(target=peer._listen_download) # Responsável pelas requisições TCP de DOWNLOAD
    alive_thread = threading.Thread(target=peer._listen_alive) # Responsável por responder requisição ALIVE 
    request_thread = threading.Thread(target=peer._request) # Responsável por fazer requisições

    # Start as thread 
    download_thread.start()
    alive_thread.start()
    request_thread.start()
